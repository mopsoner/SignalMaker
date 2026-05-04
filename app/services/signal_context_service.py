"""Context-driven signal progression helpers.

4H defines macro context and target map.
1H validates the Wyckoff/SMC setup.
15M is an execution alignment filter: it blocks only when it opposes the 1H setup.
SL and TP are structural, not fixed 2% / 5% distance filters.
"""

from __future__ import annotations

CONTEXT_TARGET_OVERLAP_PCT = 0.003
STOP_BUFFER_PCT = 0.002


def _has_context(value: dict | None) -> bool:
    if not isinstance(value, dict):
        return False
    if value.get("type") in (None, "", "none"):
        return False
    return value.get("level") is not None


def _price(signal: dict) -> float:
    return float(signal.get("price") or 0.0)


def _level_distance_pct(price: float, level: float | None) -> float | None:
    if price <= 0 or level is None:
        return None
    return abs(price - float(level)) / price


def _target_overlaps_context(target_level: float | None, context_level: float | None, price: float) -> bool:
    if target_level is None or context_level is None or price <= 0:
        return False
    return abs(float(target_level) - float(context_level)) / price <= CONTEXT_TARGET_OVERLAP_PCT


def _candidate_target(signal: dict, side: str) -> dict:
    """Pick the first directional structural liquidity target."""
    price = _price(signal)
    event_level = ((signal.get("wyckoff_event_level") or {}).get("level")
                   or (signal.get("macro_liquidity_context") or {}).get("level"))
    old_support = signal.get("old_support_shelf") or {}
    old_resistance = signal.get("old_resistance_shelf") or {}

    if side == "bear":
        raw = [
            (old_support.get("type", "old_support_shelf"), old_support.get("level"), "old_support_shelf", 90, 10),
            ("previous_day_low", signal.get("previous_day_low"), "previous_day_low", 80, 20),
            ("previous_week_low", signal.get("previous_week_low"), "previous_week_low", 72, 30),
            ("range_low", signal.get("range_low_4h"), "range_low_4h", 68, 40),
            ("major_swing_low_4h", signal.get("major_swing_low_4h"), "major_swing_low_4h", 55, 50),
        ]
        directional = lambda level: level is not None and float(level) < price
    elif side == "bull":
        raw = [
            (old_resistance.get("type", "old_resistance_shelf"), old_resistance.get("level"), "old_resistance_shelf", 90, 10),
            ("previous_day_high", signal.get("previous_day_high"), "previous_day_high", 80, 20),
            ("previous_week_high", signal.get("previous_week_high"), "previous_week_high", 72, 30),
            ("range_high", signal.get("range_high_4h"), "range_high_4h", 68, 40),
            ("major_swing_high_4h", signal.get("major_swing_high_4h"), "major_swing_high_4h", 55, 50),
        ]
        directional = lambda level: level is not None and float(level) > price
    else:
        raw = []
        directional = lambda level: False

    candidates = []
    for level_type, level, source, base_quality, hierarchy_rank in raw:
        if not directional(level):
            continue
        level = float(level)
        distance_pct = _level_distance_pct(price, level)
        if distance_pct is None:
            continue
        overlaps = _target_overlaps_context(level, event_level, price)
        score = base_quality + max(0.0, 25.0 - min(distance_pct * 100.0, 25.0)) - (100.0 if overlaps else 0.0)
        candidates.append({
            "type": level_type,
            "level": level,
            "reason": f"1h-confirmed {side} setup targets structural liquidity at {source}",
            "timeframe": "4h",
            "scope": "macro",
            "source": source,
            "distance_pct": distance_pct,
            "directional": True,
            "overlaps_context": overlaps,
            "valid": bool(not overlaps),
            "rejected_reason": "target_overlaps_context" if overlaps else None,
            "validation": "structural_liquidity_target" if not overlaps else "context_overlap",
            "score": score,
            "hierarchy_rank": hierarchy_rank,
            "projected": True,
        })

    valid = [c for c in candidates if c.get("valid")]
    if not valid:
        return {
            "valid": False,
            "reason": "missing_structural_target",
            "target_candidates": sorted(candidates, key=lambda item: (item["hierarchy_rank"], item["distance_pct"])),
        }

    selected = sorted(valid, key=lambda item: (item["hierarchy_rank"], item["distance_pct"]))[0]
    return {
        "valid": True,
        "selected": selected,
        "target_candidates": sorted(candidates, key=lambda item: (item["hierarchy_rank"], item["distance_pct"])),
    }


def _stop_candidate_list(signal: dict, refinement: dict, side: str) -> list[tuple[float | None, str, str, int]]:
    event_level = (signal.get("wyckoff_event_level") or {}).get("level")
    macro_ctx_level = (signal.get("macro_liquidity_context") or {}).get("level")
    if side == "bear":
        return [
            (refinement.get("last_high_1h"), "last_high_1h", "1H last high above UTAD/rejection", 10),
            (signal.get("range_high_1h"), "range_high_1h", "1H range high", 20),
            (event_level, "wyckoff_event_level", "4H Wyckoff event level", 30),
            (macro_ctx_level, "macro_liquidity_context", "selected 4H macro liquidity context", 40),
            (signal.get("range_high_4h"), "range_high_4h", "4H range high", 50),
            (signal.get("major_swing_high_4h"), "major_swing_high_4h", "major 4H swing high", 60),
        ]
    if side == "bull":
        return [
            (refinement.get("last_low_1h"), "last_low_1h", "1H last low below Spring/reclaim", 10),
            (signal.get("range_low_1h"), "range_low_1h", "1H range low", 20),
            (event_level, "wyckoff_event_level", "4H Wyckoff event level", 30),
            (macro_ctx_level, "macro_liquidity_context", "selected 4H macro liquidity context", 40),
            (signal.get("range_low_4h"), "range_low_4h", "4H range low", 50),
            (signal.get("major_swing_low_4h"), "major_swing_low_4h", "major 4H swing low", 60),
        ]
    return []


def _candidate_stop(signal: dict, side: str) -> dict:
    price = _price(signal)
    refinement = signal.get("refinement_context_1h") or {}
    candidates = []

    if price <= 0:
        return {"valid": False, "reason": "missing_entry_price", "stop_candidates": candidates}

    for raw_stop, source, source_reason, hierarchy_rank in _stop_candidate_list(signal, refinement, side):
        if raw_stop is None:
            continue
        raw_stop = float(raw_stop)

        if side == "bear":
            if raw_stop <= price:
                continue
            stop = raw_stop * (1.0 + STOP_BUFFER_PCT)
            method = "above_source_plus_buffer"
        elif side == "bull":
            if raw_stop >= price:
                continue
            stop = raw_stop * (1.0 - STOP_BUFFER_PCT)
            method = "below_source_minus_buffer"
        else:
            return {"valid": False, "reason": "neutral_side", "source": source, "stop_candidates": candidates}

        distance_pct = abs(price - stop) / price
        candidates.append({
            "valid": True,
            "level": stop,
            "source_level": raw_stop,
            "source": source,
            "source_reason": source_reason,
            "buffer_pct": STOP_BUFFER_PCT,
            "distance_pct": distance_pct,
            "hierarchy_rank": hierarchy_rank,
            "method": method,
            "validation": "structural_stop",
            "reason": f"hierarchical structural stop from {source}: {source_reason}",
            "rejected_reason": None,
        })

    valid_candidates = [candidate for candidate in candidates if candidate.get("valid")]
    if not valid_candidates:
        return {
            "valid": False,
            "reason": "missing_structural_stop",
            "stop_candidates": sorted(candidates, key=lambda item: (item["hierarchy_rank"], item["distance_pct"])),
        }

    selected = sorted(valid_candidates, key=lambda item: (item["hierarchy_rank"], item["distance_pct"]))[0]
    selected["stop_candidates"] = sorted(candidates, key=lambda item: (item["hierarchy_rank"], item["distance_pct"]))
    selected["selection_policy"] = "hierarchical_structural_stop"
    return selected


def _one_hour_decision(signal: dict) -> dict:
    """Validate a true 1H Wyckoff/SMC decision, not a simple retest."""
    side = "bear" if str(signal.get("bias") or "").startswith("bear") else "bull" if str(signal.get("bias") or "").startswith("bull") else "neutral"
    macro = signal.get("macro_window_4h") or {}
    refinement = signal.get("refinement_context_1h") or {}
    wyckoff = signal.get("wyckoff_requirement") or {}
    event_level = signal.get("wyckoff_event_level") or {}
    status = str(wyckoff.get("status") or "")
    reason = str(wyckoff.get("reason") or "")

    macro_ok = bool(macro.get("valid") and macro.get("side") == side)
    swept = bool(wyckoff.get("swept") or event_level.get("swept"))

    if side == "bear":
        mss_1h = bool(signal.get("mss_bear_1h") or refinement.get("mss_bear_1h"))
        bos_1h = bool(signal.get("bos_bear_1h") or refinement.get("bos_bear_1h"))
        utad = bool(refinement.get("utad_watch_1h"))
        rejection = bool("rejected" in status or "rejection" in reason or event_level.get("reclaimed"))
        valid_event = bool(utad or mss_1h or bos_1h or (swept and rejection))
        source = "1h_utad" if utad else "1h_mss_bear" if mss_1h else "1h_bos_bear" if bos_1h else "1h_sweep_rejection" if valid_event else None
    elif side == "bull":
        mss_1h = bool(signal.get("mss_bull_1h") or refinement.get("mss_bull_1h"))
        bos_1h = bool(signal.get("bos_bull_1h") or refinement.get("bos_bull_1h"))
        spring = bool(refinement.get("spring_watch_1h"))
        reclaim = bool("reclaimed" in status or "reclaim" in reason or event_level.get("reclaimed"))
        valid_event = bool(spring or mss_1h or bos_1h or (swept and reclaim))
        source = "1h_spring" if spring else "1h_mss_bull" if mss_1h else "1h_bos_bull" if bos_1h else "1h_sweep_reclaim" if valid_event else None
    else:
        mss_1h = False
        bos_1h = False
        valid_event = False
        source = None

    valid = bool(side in {"bull", "bear"} and macro_ok and valid_event)
    decision_reason = "1h_wyckoff_smc_confirmed" if valid else "waiting_1h_wyckoff_smc_event"
    if side not in {"bull", "bear"}:
        decision_reason = "neutral_bias"
    elif not macro_ok:
        decision_reason = "missing_4h_macro_side"
    elif not valid_event:
        decision_reason = "waiting_1h_sweep_reclaim_rejection_or_mss"

    return {
        "side": side,
        "valid": valid,
        "reason": decision_reason,
        "sweep_seen": swept,
        "rejection_seen": bool(side == "bear" and valid_event),
        "reclaim_seen": bool(side == "bull" and valid_event),
        "mss_seen": bool(mss_1h),
        "bos_seen": bool(bos_1h),
        "source": source,
        "cycle_filter_bypassed": True,
    }


def _execution_alignment(exec_trigger: dict) -> tuple[str, bool, bool]:
    status = exec_trigger.get("alignment_status") or ("aligned" if exec_trigger.get("valid") else "neutral_not_opposed")
    aligned = bool(exec_trigger.get("aligned") or status == "aligned")
    opposed = bool(exec_trigger.get("opposed") or status == "opposed")
    return status, aligned, opposed


def _mark_structural_plan_block(signal: dict, *, reason: str, blocked_at: str, alignment_status: str, aligned_15m: bool) -> None:
    """After a valid 1H decision, show structural planner blocks instead of stale decision_1h blocks."""
    signal["hierarchy_block_reason"] = reason
    signal["confirm_blocked_by_hierarchy"] = True
    signal["confirm_block_reason"] = reason

    gate = signal.setdefault("hierarchy_gate", {})
    gate.update({
        "accepted": False,
        "stage": signal.get("stage") or "plan_watch",
        "blocked_at": blocked_at,
        "block_reason": reason,
        "one_hour_decision_ok": True,
        "zone_1h_ok": True,
        "confirm_15m_seen": aligned_15m,
        "confirm_15m_accepted": False,
        "execution_15m_alignment": alignment_status,
        "confirmation_path": "structural_plan_watch",
    })

    for key in ("execution_trigger", "execution_trigger_5m"):
        execution_trigger = signal.get(key)
        if isinstance(execution_trigger, dict):
            execution_trigger["blocked"] = True
            execution_trigger["blocked_by"] = blocked_at
            execution_trigger["block_reason"] = reason
            execution_trigger["accepted"] = False
            execution_trigger["valid"] = False
            signal[key] = execution_trigger


def _apply_one_hour_candidate(signal: dict, confirmation: dict, confirm_ok: bool, alignment_status: str, aligned_15m: bool) -> None:
    if not confirmation.get("valid"):
        return

    side = confirmation.get("side")
    target_result = _candidate_target(signal, side)
    stop_result = _candidate_stop(signal, side)
    price = _price(signal)
    if not target_result.get("valid") or not stop_result.get("valid") or price <= 0:
        signal["one_hour_candidate_rejected"] = {
            "target": target_result,
            "stop": stop_result,
            "reason": "missing_structural_target_or_stop",
        }
        if stop_result.get("reason") == "missing_structural_stop":
            signal["planner_candidate_status"] = "rejected"
            signal["planner_candidate_reason"] = "blocked_before_planner:missing_structural_stop"
            signal["stage"] = "stop_watch"
            _mark_structural_plan_block(signal, reason="missing_structural_stop", blocked_at="stop", alignment_status=alignment_status, aligned_15m=aligned_15m)
        if target_result.get("reason") == "missing_structural_target":
            signal["planner_candidate_status"] = "rejected"
            signal["planner_candidate_reason"] = "blocked_before_planner:missing_structural_target"
            signal["stage"] = "target_watch"
            _mark_structural_plan_block(signal, reason="missing_structural_target", blocked_at="target", alignment_status=alignment_status, aligned_15m=aligned_15m)
        return

    target = target_result["selected"]
    stop = stop_result["level"]
    risk = abs(price - stop)
    reward = abs(target["level"] - price)
    if risk <= 0 or reward <= 0:
        signal["one_hour_candidate_rejected"] = {"reason": "invalid_risk_reward"}
        _mark_structural_plan_block(signal, reason="invalid_risk_reward", blocked_at="planner", alignment_status=alignment_status, aligned_15m=aligned_15m)
        return

    pipeline = signal.setdefault("pipeline", {})
    pipeline["collect"] = True
    pipeline["liquidity"] = True
    pipeline["zone"] = True
    pipeline["confirm"] = bool(confirm_ok)
    pipeline["trade"] = bool(confirm_ok)

    signal["execution_target"] = target
    signal["projected_target"] = target
    signal["confirm_source"] = signal.get("confirm_source") or confirmation.get("source")
    signal["trigger"] = signal.get("trigger") if confirm_ok else "wait"
    signal["stage"] = "trade_ready" if confirm_ok else "awaiting_15m_alignment"
    signal["state"] = "trade_ready" if confirm_ok else "awaiting_15m_alignment"
    signal["hierarchy_block_reason"] = None if confirm_ok else "waiting_15m_alignment"
    signal["confirm_blocked_by_hierarchy"] = not confirm_ok
    signal["confirm_block_reason"] = None if confirm_ok else "waiting_15m_alignment"
    signal["planner_candidate_status"] = "candidate_watch" if confirm_ok else "not_created"
    signal["planner_candidate_reason"] = None if confirm_ok else "waiting:15m_alignment"
    signal["planner_candidate_rr"] = reward / risk if confirm_ok else None
    signal["stop_source"] = stop_result

    wyckoff = signal.get("wyckoff_requirement")
    if isinstance(wyckoff, dict):
        wyckoff.setdefault("legacy_status", wyckoff.get("status"))
        wyckoff.setdefault("legacy_reason", wyckoff.get("reason"))
        wyckoff["status"] = "execution_ready" if confirm_ok else "awaiting_15m_alignment"
        wyckoff["confirmed"] = bool(confirm_ok)
        wyckoff["setup_ready"] = True
        wyckoff["reason"] = confirmation.get("source") or "1h_wyckoff_smc_confirmed"
        signal["wyckoff_requirement"] = wyckoff

    zone = signal.get("zone_validity")
    if isinstance(zone, dict):
        zone.setdefault("legacy_valid", zone.get("valid"))
        zone.setdefault("legacy_reason", zone.get("reason"))
        zone["valid"] = True
        zone["wyckoff_ok"] = True
        zone["target_ok"] = True
        zone["reason"] = "valid_1h_wyckoff_candidate"
        signal["zone_validity"] = zone

    gate = signal.setdefault("hierarchy_gate", {})
    gate.update({
        "accepted": bool(confirm_ok),
        "stage": signal["stage"],
        "blocked_at": None if confirm_ok else "15m_alignment",
        "block_reason": None if confirm_ok else "waiting_15m_alignment",
        "one_hour_decision_ok": True,
        "zone_1h_ok": True,
        "confirm_15m_seen": aligned_15m,
        "confirm_15m_accepted": bool(confirm_ok),
        "execution_15m_alignment": alignment_status,
        "confirmation_path": "1h_setup_15m_aligned" if aligned_15m and confirm_ok else "1h_setup_15m_not_opposed" if confirm_ok else "awaiting_15m_alignment",
    })

    execution_trigger = signal.get("execution_trigger")
    if isinstance(execution_trigger, dict):
        execution_trigger["blocked"] = not confirm_ok
        execution_trigger["blocked_by"] = None if confirm_ok else "15m_alignment"
        execution_trigger["block_reason"] = None if confirm_ok else "waiting_15m_alignment"
        execution_trigger["accepted"] = bool(confirm_ok)
        execution_trigger["valid"] = bool(confirm_ok)
        signal["execution_trigger"] = execution_trigger

    legacy_trigger = signal.get("execution_trigger_5m")
    if isinstance(legacy_trigger, dict):
        legacy_trigger["blocked"] = not confirm_ok
        legacy_trigger["blocked_by"] = None if confirm_ok else "15m_alignment"
        legacy_trigger["block_reason"] = None if confirm_ok else "waiting_15m_alignment"
        legacy_trigger["accepted"] = bool(confirm_ok)
        legacy_trigger["valid"] = bool(confirm_ok)
        signal["execution_trigger_5m"] = legacy_trigger

    if confirm_ok:
        signal["trade"] = {
            "status": "candidate",
            "side": "sell" if side == "bear" else "buy",
            "entry": price,
            "stop": stop,
            "stop_source": stop_result,
            "target": target["level"],
            "target_source": target,
        }


def apply_context_driven_progression(signal: dict) -> dict:
    """Progress Zone/Confirm from identified contexts before planner assessment."""
    pipeline = signal.setdefault("pipeline", {})
    macro_ctx = signal.get("macro_liquidity_context") or signal.get("liquidity_context") or {}
    entry_ctx = signal.get("entry_liquidity_context") or {}
    target = signal.get("execution_target") or signal.get("projected_target") or {}
    wyckoff = signal.get("wyckoff_requirement") or {}
    exec_trigger = signal.get("execution_trigger_5m") or signal.get("execution_trigger") or {}
    block_reason = signal.get("hierarchy_block_reason")

    context_ok = _has_context(macro_ctx) and _has_context(entry_ctx) and _has_context(target)
    setup_ready = bool(wyckoff.get("setup_ready") or wyckoff.get("confirmed"))
    alignment_status, aligned_15m, opposed_15m = _execution_alignment(exec_trigger)
    confirm_ok = bool(exec_trigger.get("accepted", exec_trigger.get("valid", False)) and not opposed_15m)
    one_hour_decision = _one_hour_decision(signal)

    signal["one_hour_decision"] = one_hour_decision
    signal["confirmation_model"] = {
        "primary_tf": "1h",
        "execution_tf": signal.get("execution_timeframe") or "15m",
        "confirmed_by_1h": bool(one_hour_decision.get("valid")),
        "confirmed_by_15m": bool(aligned_15m),
        "fifteen_min_alignment": alignment_status,
        "confirmation_source": exec_trigger.get("confirm_source") or one_hour_decision.get("source"),
        "entry_mode": "1h_setup_15m_alignment_required" if one_hour_decision.get("valid") else "wait",
    }

    if context_ok:
        pipeline["collect"] = True
        pipeline["liquidity"] = True
        pipeline["zone"] = True
        if setup_ready or confirm_ok or one_hour_decision.get("valid"):
            zone_validity = signal.setdefault("zone_validity", {})
            zone_validity["valid"] = True
            zone_validity["target_ok"] = True
            zone_validity["wyckoff_ok"] = bool(setup_ready or one_hour_decision.get("valid") or zone_validity.get("wyckoff_ok"))
            zone_validity["reason"] = "valid_context_zone"
            if signal.get("zone_quality") == "weak":
                signal["zone_quality"] = "medium"

    if context_ok and confirm_ok:
        pipeline["confirm"] = True
        signal["trigger"] = exec_trigger.get("trigger") or signal.get("trigger")

    if context_ok and block_reason in {"blocked_no_4h_bull_window", "blocked_no_4h_bear_window"}:
        signal["hierarchy_block_reason"] = None
        if signal.get("planner_candidate_reason") == block_reason:
            signal["planner_candidate_reason"] = None
        if isinstance(wyckoff, dict) and wyckoff.get("status") == "blocked":
            wyckoff["status"] = "context_ready"
            wyckoff["reason"] = "context identified; 4h window kept as diagnostic only"

    signal.setdefault("hierarchy_gate", {})["confirmation_path"] = (
        "1h_setup_15m_aligned" if one_hour_decision.get("valid") and aligned_15m and confirm_ok
        else "1h_setup_15m_not_opposed" if one_hour_decision.get("valid") and confirm_ok
        else "awaiting_15m_alignment" if one_hour_decision.get("valid")
        else "waiting"
    )

    if context_ok and one_hour_decision.get("valid"):
        _apply_one_hour_candidate(signal, one_hour_decision, confirm_ok, alignment_status, aligned_15m)
        if not confirm_ok:
            signal["score"] = float(signal.get("score") or 0.0) + 1.0
            final_breakdown = signal.setdefault("final_score_breakdown", {})
            final_breakdown["one_hour_confirm"] = final_breakdown.get("one_hour_confirm", 0) + 1.0
        else:
            signal["score"] = float(signal.get("score") or 0.0) + 1.0
            final_breakdown = signal.setdefault("final_score_breakdown", {})
            final_breakdown["one_hour_confirm"] = final_breakdown.get("one_hour_confirm", 0) + 1.0

    return signal

"""Context-driven signal progression helpers.

Rules:
- 4H defines macro context and target map.
- 1H validates the Wyckoff/SMC setup.
- 15M is an alignment filter: aligned or neutral_not_opposed is tradable; opposed blocks.
- SL/TP are structural. Stop buffer is applied before stop validation.
- Debug fields stay JSON-safe and shallow to avoid recursive payload serialization.
"""

from __future__ import annotations

CONTEXT_TARGET_OVERLAP_PCT = 0.003
STOP_BUFFER_PCT = 0.002
MAX_DEBUG_CANDIDATES = 6


def _as_float(value):
    if isinstance(value, dict):
        value = value.get("level")
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _price(signal: dict) -> float:
    return float(signal.get("price") or 0.0)


def _has_context(value: dict | None) -> bool:
    return isinstance(value, dict) and value.get("type") not in (None, "", "none") and value.get("level") is not None


def _distance_pct(entry: float, level: float | None) -> float | None:
    if entry <= 0 or level is None:
        return None
    return abs(float(level) - entry) / entry


def _side(signal: dict) -> str:
    bias = str(signal.get("bias") or "").lower()
    if bias.startswith("bear"):
        return "bear"
    if bias.startswith("bull"):
        return "bull"
    gate_side = str((signal.get("hierarchy_gate") or {}).get("side") or "").lower()
    if gate_side in {"bear", "bull"}:
        return gate_side
    return "neutral"


def _alignment(exec_trigger: dict) -> tuple[str, bool, bool]:
    status = exec_trigger.get("alignment_status") or ("opposed" if exec_trigger.get("opposed") else "aligned" if exec_trigger.get("aligned") or exec_trigger.get("valid") else "neutral_not_opposed")
    aligned = bool(exec_trigger.get("aligned") or status == "aligned")
    opposed = bool(exec_trigger.get("opposed") or status == "opposed")
    return status, aligned, opposed


def _shallow_candidates(items: list[dict]) -> list[dict]:
    safe = []
    for item in (items or [])[:MAX_DEBUG_CANDIDATES]:
        safe.append({
            "source": item.get("source"),
            "type": item.get("type"),
            "level": item.get("level"),
            "source_level": item.get("source_level"),
            "buffered_level": item.get("buffered_level"),
            "distance_pct": item.get("distance_pct"),
            "hierarchy_rank": item.get("hierarchy_rank"),
            "validation": item.get("validation"),
            "method": item.get("method"),
            "buffer_pct": item.get("buffer_pct"),
            "valid": item.get("valid"),
            "rejected_reason": item.get("rejected_reason"),
        })
    return safe


def _target_candidates(signal: dict, side: str) -> list[dict]:
    entry = _price(signal)
    if entry <= 0:
        return []
    event_level = _as_float(signal.get("wyckoff_event_level")) or _as_float(signal.get("macro_liquidity_context"))
    old_support = signal.get("old_support_shelf") or {}
    old_resistance = signal.get("old_resistance_shelf") or {}
    if side == "bear":
        raw = [
            (old_support.get("type") or "old_support_shelf", old_support, "old_support_shelf", 90, 10),
            ("previous_day_low", signal.get("previous_day_low"), "previous_day_low", 80, 20),
            ("previous_week_low", signal.get("previous_week_low"), "previous_week_low", 72, 30),
            ("range_low", signal.get("range_low_4h"), "range_low_4h", 68, 40),
            ("major_swing_low_4h", signal.get("major_swing_low_4h"), "major_swing_low_4h", 55, 50),
        ]
        directional = lambda lvl: lvl is not None and lvl < entry
    elif side == "bull":
        raw = [
            (old_resistance.get("type") or "old_resistance_shelf", old_resistance, "old_resistance_shelf", 90, 10),
            ("previous_day_high", signal.get("previous_day_high"), "previous_day_high", 80, 20),
            ("previous_week_high", signal.get("previous_week_high"), "previous_week_high", 72, 30),
            ("range_high", signal.get("range_high_4h"), "range_high_4h", 68, 40),
            ("major_swing_high_4h", signal.get("major_swing_high_4h"), "major_swing_high_4h", 55, 50),
        ]
        directional = lambda lvl: lvl is not None and lvl > entry
    else:
        return []

    out = []
    seen = set()
    for typ, value, source, base_score, rank in raw:
        level = _as_float(value)
        if level is None or not directional(level):
            continue
        key = round(level, 12)
        if key in seen:
            continue
        seen.add(key)
        dist = _distance_pct(entry, level)
        overlaps = bool(event_level and abs(level - event_level) / entry <= CONTEXT_TARGET_OVERLAP_PCT)
        out.append({
            "type": typ,
            "level": level,
            "reason": f"1h-confirmed {side} setup targets structural liquidity at {source}",
            "timeframe": "4h",
            "scope": "macro",
            "source": source,
            "distance_pct": dist,
            "directional": True,
            "overlaps_context": overlaps,
            "valid": not overlaps,
            "rejected_reason": "target_overlaps_context" if overlaps else None,
            "validation": "structural_liquidity_target" if not overlaps else "context_overlap",
            "score": base_score + max(0.0, 25.0 - min((dist or 0) * 100.0, 25.0)) - (100.0 if overlaps else 0.0),
            "hierarchy_rank": rank,
            "projected": True,
        })
    return sorted(out, key=lambda item: (not item.get("valid"), item["hierarchy_rank"], item.get("distance_pct") or 999))


def _candidate_target(signal: dict, side: str) -> dict:
    candidates = _target_candidates(signal, side)
    valid = [item for item in candidates if item.get("valid")]
    if not valid:
        return {"valid": False, "reason": "missing_structural_target", "target_candidates": _shallow_candidates(candidates)}
    selected = dict(valid[0])
    return {"valid": True, "selected": selected, "target_candidates": _shallow_candidates(candidates)}


def _stop_sources(signal: dict, side: str) -> list[tuple[str, object, str, int]]:
    refinement = signal.get("refinement_context_1h") or {}
    event_level = signal.get("wyckoff_event_level")
    macro_context = signal.get("macro_liquidity_context") or signal.get("liquidity_context")
    entry_context = signal.get("entry_liquidity_context")
    if side == "bear":
        return [
            ("entry_liquidity_context", entry_context, "nearest 1H sell context", 5),
            ("last_high_1h", refinement.get("last_high_1h"), "1H last high above UTAD/rejection", 10),
            ("range_high_1h", signal.get("range_high_1h"), "1H range high", 20),
            ("external_swing_high", signal.get("external_swing_high"), "external swing high", 30),
            ("internal_bear_pivot_high", signal.get("internal_bear_pivot_high"), "internal bear pivot high", 40),
            ("wyckoff_event_level", event_level, "4H Wyckoff event level", 50),
            ("macro_liquidity_context", macro_context, "selected 4H macro context", 60),
            ("range_high_4h", signal.get("range_high_4h"), "4H range high", 70),
            ("major_swing_high_4h", signal.get("major_swing_high_4h"), "major 4H swing high", 80),
        ]
    if side == "bull":
        return [
            ("entry_liquidity_context", entry_context, "nearest 1H buy context", 5),
            ("last_low_1h", refinement.get("last_low_1h"), "1H last low below Spring/reclaim", 10),
            ("range_low_1h", signal.get("range_low_1h"), "1H range low", 20),
            ("external_swing_low", signal.get("external_swing_low"), "external swing low", 30),
            ("internal_bull_pivot_low", signal.get("internal_bull_pivot_low"), "internal bull pivot low", 40),
            ("wyckoff_event_level", event_level, "4H Wyckoff event level", 50),
            ("macro_liquidity_context", macro_context, "selected 4H macro context", 60),
            ("range_low_4h", signal.get("range_low_4h"), "4H range low", 70),
            ("major_swing_low_4h", signal.get("major_swing_low_4h"), "major 4H swing low", 80),
        ]
    return []


def _candidate_stop(signal: dict, side: str) -> dict:
    entry = _price(signal)
    if entry <= 0:
        return {"valid": False, "reason": "missing_entry_price", "stop_candidates": []}
    candidates = []
    rejected = []
    for source, value, source_reason, rank in _stop_sources(signal, side):
        source_level = _as_float(value)
        if source_level is None:
            continue
        if side == "bear":
            stop = source_level * (1.0 + STOP_BUFFER_PCT)
            method = "above_source_plus_buffer"
            if stop <= entry:
                rejected.append({
                    "source": source,
                    "source_level": source_level,
                    "buffered_level": stop,
                    "distance_pct": _distance_pct(entry, stop),
                    "hierarchy_rank": rank,
                    "method": method,
                    "buffer_pct": STOP_BUFFER_PCT,
                    "valid": False,
                    "rejected_reason": "buffered_stop_not_above_entry",
                })
                continue
        elif side == "bull":
            stop = source_level * (1.0 - STOP_BUFFER_PCT)
            method = "below_source_minus_buffer"
            if stop >= entry:
                rejected.append({
                    "source": source,
                    "source_level": source_level,
                    "buffered_level": stop,
                    "distance_pct": _distance_pct(entry, stop),
                    "hierarchy_rank": rank,
                    "method": method,
                    "buffer_pct": STOP_BUFFER_PCT,
                    "valid": False,
                    "rejected_reason": "buffered_stop_not_below_entry",
                })
                continue
        else:
            continue
        candidates.append({
            "valid": True,
            "level": stop,
            "source_level": source_level,
            "source": source,
            "source_reason": source_reason,
            "buffer_pct": STOP_BUFFER_PCT,
            "distance_pct": _distance_pct(entry, stop),
            "hierarchy_rank": rank,
            "method": method,
            "validation": "structural_stop",
            "reason": f"hierarchical structural stop from {source}: {source_reason}",
            "rejected_reason": None,
        })
    candidates = sorted(candidates, key=lambda item: (item["hierarchy_rank"], item.get("distance_pct") or 999))
    rejected = sorted(rejected, key=lambda item: (item["hierarchy_rank"], abs(item.get("distance_pct") or 999)))
    if not candidates:
        reason = "missing_structural_stop"
        if rejected:
            reason = "price_extended_above_structural_stop_sources" if side == "bear" else "price_extended_below_structural_stop_sources"
        return {"valid": False, "reason": reason, "stop_candidates": [], "rejected_stop_candidates": _shallow_candidates(rejected)}
    selected = dict(candidates[0])
    selected["stop_candidates"] = _shallow_candidates(candidates)
    selected["rejected_stop_candidates"] = _shallow_candidates(rejected)
    selected["selection_policy"] = "hierarchical_structural_stop_buffer_first"
    return selected


def _one_hour_decision(signal: dict) -> dict:
    side = _side(signal)
    macro = signal.get("macro_window_4h") or {}
    refinement = signal.get("refinement_context_1h") or {}
    wyckoff = signal.get("wyckoff_requirement") or {}
    event_level = signal.get("wyckoff_event_level") or {}
    status = str(wyckoff.get("status") or "")
    reason = str(wyckoff.get("reason") or "")
    macro_ok = bool(macro.get("valid") and macro.get("side") == side)
    swept = bool(wyckoff.get("swept") or event_level.get("swept"))
    if side == "bear":
        mss = bool(signal.get("mss_bear_1h") or refinement.get("mss_bear_1h"))
        bos = bool(signal.get("bos_bear_1h") or refinement.get("bos_bear_1h"))
        utad = bool(refinement.get("utad_watch_1h"))
        rejection = bool("rejected" in status or "rejection" in reason or event_level.get("reclaimed"))
        valid_event = bool(utad or mss or bos or (swept and rejection))
        source = "1h_utad" if utad else "1h_mss_bear" if mss else "1h_bos_bear" if bos else "1h_sweep_rejection" if valid_event else None
    elif side == "bull":
        mss = bool(signal.get("mss_bull_1h") or refinement.get("mss_bull_1h"))
        bos = bool(signal.get("bos_bull_1h") or refinement.get("bos_bull_1h"))
        spring = bool(refinement.get("spring_watch_1h"))
        reclaim = bool("reclaimed" in status or "reclaim" in reason or event_level.get("reclaimed"))
        valid_event = bool(spring or mss or bos or (swept and reclaim))
        source = "1h_spring" if spring else "1h_mss_bull" if mss else "1h_bos_bull" if bos else "1h_sweep_reclaim" if valid_event else None
    else:
        mss = bos = valid_event = False
        source = None
    valid = bool(side in {"bear", "bull"} and macro_ok and valid_event)
    return {"side": side, "valid": valid, "reason": "1h_wyckoff_smc_confirmed" if valid else "waiting_1h_sweep_reclaim_rejection_or_mss", "sweep_seen": swept, "rejection_seen": bool(side == "bear" and valid_event), "reclaim_seen": bool(side == "bull" and valid_event), "mss_seen": bool(mss), "bos_seen": bool(bos), "source": source, "cycle_filter_bypassed": True}


def _side_fields(side: str) -> dict:
    if side == "bear":
        return {"side": "short", "position_side": "short", "entry_action": "sell", "exit_action": "buy", "order_side": "sell", "side_label": "SHORT"}
    return {"side": "long", "position_side": "long", "entry_action": "buy", "exit_action": "sell", "order_side": "buy", "side_label": "LONG"}


def _mark_block(signal: dict, *, reason: str, blocked_at: str, alignment_status: str, aligned_15m: bool) -> None:
    signal["hierarchy_block_reason"] = reason
    signal["confirm_blocked_by_hierarchy"] = True
    signal["confirm_block_reason"] = reason
    gate = signal.setdefault("hierarchy_gate", {})
    gate.update({"accepted": False, "stage": signal.get("stage") or "plan_watch", "blocked_at": blocked_at, "block_reason": reason, "one_hour_decision_ok": True, "zone_1h_ok": True, "confirm_15m_seen": aligned_15m, "confirm_15m_accepted": False, "execution_15m_alignment": alignment_status, "confirmation_path": "structural_plan_watch"})
    for key in ("execution_trigger", "execution_trigger_5m"):
        obj = signal.get(key)
        if isinstance(obj, dict):
            obj.update({"blocked": True, "blocked_by": blocked_at, "block_reason": reason, "accepted": False, "valid": False})
            signal[key] = obj


def _apply_one_hour_candidate(signal: dict, decision: dict, confirm_ok: bool, alignment_status: str, aligned_15m: bool) -> None:
    side = decision.get("side")
    target_result = _candidate_target(signal, side)
    stop_result = _candidate_stop(signal, side)
    entry = _price(signal)
    if not target_result.get("valid") or not stop_result.get("valid") or entry <= 0:
        stop_reason = stop_result.get("reason") or "missing_structural_stop"
        target_reason = target_result.get("reason") or "missing_structural_target"
        signal["one_hour_candidate_rejected"] = {"target": target_result, "stop": stop_result, "reason": "missing_structural_target_or_stop"}
        if not stop_result.get("valid"):
            signal["planner_candidate_status"] = "rejected"
            signal["planner_candidate_reason"] = f"blocked_before_planner:{stop_reason}"
            signal["stage"] = "stop_watch"
            _mark_block(signal, reason=stop_reason, blocked_at="stop", alignment_status=alignment_status, aligned_15m=aligned_15m)
        elif not target_result.get("valid"):
            signal["planner_candidate_status"] = "rejected"
            signal["planner_candidate_reason"] = f"blocked_before_planner:{target_reason}"
            signal["stage"] = "target_watch"
            _mark_block(signal, reason=target_reason, blocked_at="target", alignment_status=alignment_status, aligned_15m=aligned_15m)
        return
    target = target_result["selected"]
    stop = stop_result["level"]
    risk = abs(entry - stop)
    reward = abs(target["level"] - entry)
    if risk <= 0 or reward <= 0:
        signal["one_hour_candidate_rejected"] = {"reason": "invalid_risk_reward", "target": target_result, "stop": stop_result}
        _mark_block(signal, reason="invalid_risk_reward", blocked_at="planner", alignment_status=alignment_status, aligned_15m=aligned_15m)
        return
    pipeline = signal.setdefault("pipeline", {})
    pipeline.update({"collect": True, "liquidity": True, "zone": True, "confirm": bool(confirm_ok), "trade": bool(confirm_ok)})
    signal["execution_target"] = target
    signal["projected_target"] = target
    signal["stop_source"] = stop_result.get("source")
    signal["stop_debug"] = {"selected": {k: stop_result.get(k) for k in ("source", "source_level", "level", "distance_pct", "buffer_pct", "method", "validation")}, "candidates": stop_result.get("stop_candidates", []), "rejected": stop_result.get("rejected_stop_candidates", [])}
    signal["planner_candidate_status"] = "candidate_watch" if confirm_ok else "not_created"
    signal["planner_candidate_reason"] = None if confirm_ok else "waiting:15m_alignment"
    signal["planner_candidate_rr"] = reward / risk if confirm_ok else None
    signal["stage"] = "trade_ready" if confirm_ok else "awaiting_15m_alignment"
    signal["state"] = signal["stage"]
    signal["hierarchy_block_reason"] = None if confirm_ok else "waiting_15m_alignment"
    signal["confirm_blocked_by_hierarchy"] = not confirm_ok
    signal["confirm_block_reason"] = None if confirm_ok else "waiting_15m_alignment"
    gate = signal.setdefault("hierarchy_gate", {})
    gate.update({"accepted": bool(confirm_ok), "stage": signal["stage"], "blocked_at": None if confirm_ok else "15m_alignment", "block_reason": None if confirm_ok else "waiting_15m_alignment", "one_hour_decision_ok": True, "zone_1h_ok": True, "confirm_15m_seen": aligned_15m, "confirm_15m_accepted": bool(confirm_ok), "execution_15m_alignment": alignment_status, "confirmation_path": "1h_setup_15m_aligned" if aligned_15m and confirm_ok else "1h_setup_15m_not_opposed" if confirm_ok else "awaiting_15m_alignment"})
    for key in ("execution_trigger", "execution_trigger_5m"):
        obj = signal.get(key)
        if isinstance(obj, dict):
            obj.update({"blocked": not confirm_ok, "blocked_by": None if confirm_ok else "15m_alignment", "block_reason": None if confirm_ok else "waiting_15m_alignment", "accepted": bool(confirm_ok), "valid": bool(confirm_ok)})
            signal[key] = obj
    wyckoff = signal.get("wyckoff_requirement")
    if isinstance(wyckoff, dict):
        wyckoff.update({"status": "execution_ready" if confirm_ok else "awaiting_15m_alignment", "confirmed": bool(confirm_ok), "setup_ready": True, "reason": decision.get("source") or "1h_wyckoff_smc_confirmed"})
    zone = signal.setdefault("zone_validity", {})
    if isinstance(zone, dict):
        zone.update({"valid": True, "wyckoff_ok": True, "target_ok": True, "reason": "valid_1h_wyckoff_candidate"})
    if confirm_ok:
        signal["trade"] = {"status": "candidate", **_side_fields(side), "entry": entry, "stop": stop, "stop_source": stop_result.get("source"), "stop_debug": signal["stop_debug"], "target": target["level"], "target_source": target.get("source"), "target_debug": {k: target.get(k) for k in ("type", "source", "level", "distance_pct", "validation")}}


def apply_context_driven_progression(signal: dict) -> dict:
    pipeline = signal.setdefault("pipeline", {})
    macro_ctx = signal.get("macro_liquidity_context") or signal.get("liquidity_context") or {}
    entry_ctx = signal.get("entry_liquidity_context") or {}
    target = signal.get("execution_target") or signal.get("projected_target") or {}
    exec_trigger = signal.get("execution_trigger_5m") or signal.get("execution_trigger") or {}
    context_ok = _has_context(macro_ctx) and _has_context(entry_ctx) and _has_context(target)
    alignment_status, aligned_15m, opposed_15m = _alignment(exec_trigger)
    decision = _one_hour_decision(signal)
    confirm_ok = bool(decision.get("valid") and not opposed_15m)
    signal["one_hour_decision"] = decision
    signal["confirmation_model"] = {"primary_tf": "1h", "execution_tf": signal.get("execution_timeframe") or "15m", "confirmed_by_1h": bool(decision.get("valid")), "confirmed_by_15m": bool(aligned_15m), "fifteen_min_alignment": alignment_status, "confirmation_source": exec_trigger.get("confirm_source") or decision.get("source"), "entry_mode": "1h_setup_15m_alignment_required" if decision.get("valid") else "wait"}
    if context_ok:
        pipeline.update({"collect": True, "liquidity": True, "zone": True})
        zone = signal.setdefault("zone_validity", {})
        if isinstance(zone, dict):
            zone.update({"valid": True, "target_ok": True, "wyckoff_ok": bool(decision.get("valid") or zone.get("wyckoff_ok")), "reason": "valid_context_zone"})
    signal.setdefault("hierarchy_gate", {})["confirmation_path"] = ("1h_setup_15m_aligned" if decision.get("valid") and aligned_15m and confirm_ok else "1h_setup_15m_not_opposed" if decision.get("valid") and confirm_ok else "awaiting_15m_alignment" if decision.get("valid") else "waiting")
    if context_ok and decision.get("valid"):
        _apply_one_hour_candidate(signal, decision, confirm_ok, alignment_status, aligned_15m)
        signal["score"] = float(signal.get("score") or 0.0) + 1.0
        final_breakdown = signal.setdefault("final_score_breakdown", {})
        final_breakdown["one_hour_confirm"] = final_breakdown.get("one_hour_confirm", 0) + 1.0
    return signal

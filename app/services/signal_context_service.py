"""Context-driven signal progression helpers.

The 4H macro window is diagnostic/scoring context. Once the engine has already
identified macro liquidity, entry liquidity and a target, the setup can progress
to Zone/Confirm based on those contexts and the local execution trigger.

This module also adds the SignalMaker execution rule discussed in the product
logic: 4H defines the macro side/target, 1H can validate the Wyckoff/SMC idea,
and 15m remains an execution-quality upgrade instead of the only possible gate.
"""

from __future__ import annotations

MIN_TARGET_DISTANCE_PCT = 0.003
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
    return abs(float(target_level) - float(context_level)) / price <= MIN_TARGET_DISTANCE_PCT


def _candidate_target(signal: dict, side: str) -> dict:
    """Pick a directional 4H target and avoid overlap with the event/context level."""
    price = _price(signal)
    event_level = ((signal.get("wyckoff_event_level") or {}).get("level")
                   or (signal.get("macro_liquidity_context") or {}).get("level"))
    old_support = signal.get("old_support_shelf") or {}
    old_resistance = signal.get("old_resistance_shelf") or {}

    if side == "bear":
        raw = [
            ("range_low", signal.get("range_low_4h"), "range_low_4h", 90),
            ("major_swing_low_4h", signal.get("major_swing_low_4h"), "major_swing_low_4h", 80),
            (old_support.get("type", "old_support_shelf"), old_support.get("level"), "old_support_shelf", 75),
            ("previous_day_low", signal.get("previous_day_low"), "previous_day_low", 60),
            ("previous_week_low", signal.get("previous_week_low"), "previous_week_low", 55),
        ]
        directional = lambda level: level is not None and float(level) < price
    elif side == "bull":
        raw = [
            ("range_high", signal.get("range_high_4h"), "range_high_4h", 90),
            ("major_swing_high_4h", signal.get("major_swing_high_4h"), "major_swing_high_4h", 80),
            (old_resistance.get("type", "old_resistance_shelf"), old_resistance.get("level"), "old_resistance_shelf", 75),
            ("previous_day_high", signal.get("previous_day_high"), "previous_day_high", 60),
            ("previous_week_high", signal.get("previous_week_high"), "previous_week_high", 55),
        ]
        directional = lambda level: level is not None and float(level) > price
    else:
        raw = []
        directional = lambda level: False

    candidates = []
    for level_type, level, source, base_quality in raw:
        if not directional(level):
            continue
        level = float(level)
        distance_pct = _level_distance_pct(price, level)
        if distance_pct is None or distance_pct < MIN_TARGET_DISTANCE_PCT:
            continue
        overlaps = _target_overlaps_context(level, event_level, price)
        score = base_quality + min(distance_pct * 100.0, 25.0) - (100.0 if overlaps else 0.0)
        candidates.append({
            "type": level_type,
            "level": level,
            "reason": f"1h-confirmed {side} setup targets {source}",
            "timeframe": "4h",
            "scope": "macro",
            "source": source,
            "distance_pct": distance_pct,
            "directional": True,
            "overlaps_context": overlaps,
            "score": score,
            "projected": True,
        })

    valid = [c for c in candidates if not c["overlaps_context"]]
    if not valid:
        return {
            "valid": False,
            "reason": "no_valid_directional_4h_target",
            "target_candidates": candidates,
        }

    selected = sorted(valid, key=lambda item: item["score"], reverse=True)[0]
    return {
        "valid": True,
        "selected": selected,
        "target_candidates": candidates,
    }


def _candidate_stop(signal: dict, side: str) -> dict:
    price = _price(signal)
    refinement = signal.get("refinement_context_1h") or {}
    event_level = (signal.get("wyckoff_event_level") or {}).get("level")
    if side == "bear":
        raw_stop = refinement.get("last_high_1h") or signal.get("range_high_1h") or event_level
        if raw_stop is None:
            return {"valid": False, "reason": "missing_bear_stop_level"}
        stop = float(raw_stop) * (1.0 + STOP_BUFFER_PCT)
        if stop <= price:
            stop = price * (1.0 + STOP_BUFFER_PCT)
    elif side == "bull":
        raw_stop = refinement.get("last_low_1h") or signal.get("range_low_1h") or event_level
        if raw_stop is None:
            return {"valid": False, "reason": "missing_bull_stop_level"}
        stop = float(raw_stop) * (1.0 - STOP_BUFFER_PCT)
        if stop >= price:
            stop = price * (1.0 - STOP_BUFFER_PCT)
    else:
        return {"valid": False, "reason": "neutral_side"}
    return {"valid": True, "level": stop, "source_level": raw_stop}


def _one_hour_confirmation(signal: dict) -> dict:
    """Validate a true 1H Wyckoff/SMC event, not a simple retest.

    1H is now the decision timeframe. 4H keeps the directional context and the
    target selection, while mid/late-cycle labels are diagnostic only. A confirmed
    1H Spring/UTAD/MSS/BOS should therefore be allowed to create a candidate even
    when the old cycle label would have been mid_cycle or late_cycle.
    """
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
    debug_reason = "1h_wyckoff_smc_confirmed" if valid else "waiting_1h_wyckoff_smc_event"
    if side not in {"bull", "bear"}:
        debug_reason = "neutral_bias"
    elif not macro_ok:
        debug_reason = "missing_4h_macro_side"
    elif not valid_event:
        debug_reason = "waiting_1h_sweep_reclaim_rejection_or_mss"

    return {
        "side": side,
        "valid": valid,
        "reason": debug_reason,
        "sweep_seen": swept,
        "rejection_seen": bool(side == "bear" and valid_event),
        "reclaim_seen": bool(side == "bull" and valid_event),
        "mss_seen": bool(mss_1h),
        "bos_seen": bool(bos_1h),
        "source": source,
        "cycle_filter_bypassed": True,
    }


def _apply_one_hour_candidate(signal: dict, confirmation: dict, confirm_ok: bool) -> None:
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
            "reason": "missing_target_or_stop",
        }
        return

    target = target_result["selected"]
    stop = stop_result["level"]
    risk = abs(price - stop)
    reward = abs(target["level"] - price)
    if risk <= 0 or reward <= 0:
        signal["one_hour_candidate_rejected"] = {"reason": "invalid_risk_reward"}
        return

    pipeline = signal.setdefault("pipeline", {})
    pipeline["collect"] = True
    pipeline["liquidity"] = True
    pipeline["zone"] = True
    pipeline["confirm"] = True

    signal["execution_target"] = target
    signal["projected_target"] = target
    signal["confirm_source"] = signal.get("confirm_source") or confirmation.get("source")
    signal["trigger"] = signal.get("trigger") if confirm_ok else "1h_confirm_15m_optional"
    signal["stage"] = "trade_ready" if confirm_ok else "trade_candidate"
    signal["state"] = "trade_ready" if confirm_ok else "trade_candidate"
    signal["hierarchy_block_reason"] = None
    signal["confirm_blocked_by_hierarchy"] = False
    signal["confirm_block_reason"] = None
    signal["planner_candidate_status"] = "candidate_watch"
    signal["planner_candidate_reason"] = None
    signal["planner_candidate_rr"] = reward / risk
    signal["trade"] = {
        "status": "candidate",
        "side": "sell" if side == "bear" else "buy",
        "entry": price,
        "stop": stop,
        "target": target["level"],
    }


def apply_context_driven_progression(signal: dict) -> dict:
    """Progress Zone/Confirm from identified contexts instead of 4H hard gate.

    This replaces the previous package-level monkey patch. It is an explicit
    post-engine normalization step used by the pipeline before planner assessment.
    """
    pipeline = signal.setdefault("pipeline", {})
    macro_ctx = signal.get("macro_liquidity_context") or signal.get("liquidity_context") or {}
    entry_ctx = signal.get("entry_liquidity_context") or {}
    target = signal.get("execution_target") or signal.get("projected_target") or {}
    wyckoff = signal.get("wyckoff_requirement") or {}
    exec_trigger = signal.get("execution_trigger_5m") or signal.get("execution_trigger") or {}
    block_reason = signal.get("hierarchy_block_reason")

    context_ok = _has_context(macro_ctx) and _has_context(entry_ctx) and _has_context(target)
    setup_ready = bool(wyckoff.get("setup_ready") or wyckoff.get("confirmed"))
    confirm_ok = bool(exec_trigger.get("valid"))
    one_hour_confirmation = _one_hour_confirmation(signal)

    signal["one_hour_confirmation_debug"] = one_hour_confirmation
    signal["confirmation_model"] = {
        "primary_tf": "1h",
        "execution_tf": signal.get("execution_timeframe") or "15m",
        "confirmed_by_1h": bool(one_hour_confirmation.get("valid")),
        "confirmed_by_15m": confirm_ok,
        "confirmation_source": exec_trigger.get("confirm_source") or one_hour_confirmation.get("source"),
        "entry_mode": "15m_confirmed" if confirm_ok else "1h_confirm_15m_optional" if one_hour_confirmation.get("valid") else "wait",
    }

    if context_ok:
        pipeline["collect"] = True
        pipeline["liquidity"] = True
        pipeline["zone"] = True
        if setup_ready or confirm_ok or one_hour_confirmation.get("valid"):
            zone_validity = signal.setdefault("zone_validity", {})
            zone_validity["valid"] = True
            zone_validity["target_ok"] = True
            zone_validity["wyckoff_ok"] = bool(setup_ready or one_hour_confirmation.get("valid") or zone_validity.get("wyckoff_ok"))
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
        "1h_primary_15m_optional" if one_hour_confirmation.get("valid") and not confirm_ok
        else "1h_plus_15m" if one_hour_confirmation.get("valid") and confirm_ok
        else "15m_only" if confirm_ok
        else "waiting"
    )

    if context_ok and one_hour_confirmation.get("valid"):
        _apply_one_hour_candidate(signal, one_hour_confirmation, confirm_ok)
        # Small score normalization after the engine score has already been computed.
        if not confirm_ok:
            signal["score"] = float(signal.get("score") or 0.0) + 2.0
            final_breakdown = signal.setdefault("final_score_breakdown", {})
            final_breakdown["one_hour_confirm"] = final_breakdown.get("one_hour_confirm", 0) + 2.0
        else:
            signal["score"] = float(signal.get("score") or 0.0) + 1.0
            final_breakdown = signal.setdefault("final_score_breakdown", {})
            final_breakdown["one_hour_confirm"] = final_breakdown.get("one_hour_confirm", 0) + 1.0

    return signal

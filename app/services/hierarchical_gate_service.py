"""Hierarchical SignalMaker gates for Wyckoff + SMC progression.

4H  -> macro context / target selection only
1H  -> decision timeframe: Spring / UTAD / MSS / BOS / reclaim / rejection
15M -> optional execution-quality confirmation

The old zone_1h gate is intentionally removed. 1H validation is handled by the
Wyckoff/SMC event model upstream, then this service only decides whether the
setup should wait for a real 1H event, wait for optional 15m timing, or allow the
planner to create a candidate.
"""

EXECUTION_TIMEFRAME = "15m"
LEGACY_EXECUTION_TRIGGER_KEY = "execution_trigger_5m"
MIN_TARGET_DISTANCE_PCT = 0.008
CONTEXT_TARGET_OVERLAP_PCT = 0.003
CONTEXT_TOO_FAR_PCT = 0.18

WYCKOFF_EXECUTION_READY_STATUSES = {
    "execution_ready",
    "1h_confirmed_15m_optional",
    "rejected_waiting_5m_confirm",
    "reclaimed_waiting_5m_confirm",
    "rejected_waiting_15m_confirm",
    "reclaimed_waiting_15m_confirm",
}
WYCKOFF_SETUP_READY_STATUSES = WYCKOFF_EXECUTION_READY_STATUSES | {
    "swept_waiting_rejection",
    "swept_waiting_reclaim",
}

PRE_LIQUIDITY_STAGES = {
    "macro_watch",
    "context_invalid",
    "context_target_overlap",
    "entry_context_watch",
    "target_watch",
}
PRE_ZONE_STAGES = PRE_LIQUIDITY_STAGES | {"liquidity_watch"}
PRE_CONFIRM_STAGES = PRE_ZONE_STAGES | {"waiting_1h_event", "confirm_watch"}


def _bias_side(signal: dict) -> str:
    bias = str(signal.get("bias") or "neutral")
    if bias.startswith("bear"):
        return "bear"
    if bias.startswith("bull"):
        return "bull"
    return "neutral"


def _has_level(value) -> bool:
    return isinstance(value, dict) and value.get("level") is not None


def _level(value):
    if isinstance(value, dict):
        raw = value.get("level")
    else:
        raw = value
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _price(signal: dict) -> float:
    try:
        return float(signal.get("price") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _clean_trigger_source(source):
    if source in {None, "", "none"}:
        return None
    if source == "5m_bos":
        return "15m_bos"
    return source


def _near(price: float, level, pct: float) -> bool:
    if price <= 0 or level is None:
        return False
    return abs(price - float(level)) / price <= pct


def _side_structure_seen(signal: dict, side: str) -> bool:
    if side == "bull":
        return bool(signal.get("mss_bull") or signal.get("bos_bull"))
    if side == "bear":
        return bool(signal.get("mss_bear") or signal.get("bos_bear"))
    return bool(signal.get("mss_bull") or signal.get("bos_bull") or signal.get("mss_bear") or signal.get("bos_bear"))


def _execution_source(signal: dict):
    legacy_trigger = signal.get(LEGACY_EXECUTION_TRIGGER_KEY) or {}
    public_trigger = signal.get("execution_trigger") or {}
    return _clean_trigger_source(
        legacy_trigger.get("confirm_source")
        or public_trigger.get("confirm_source")
        or signal.get("confirm_source")
    )


def _execution_seen(signal: dict) -> bool:
    side = _bias_side(signal)
    legacy_trigger = signal.get(LEGACY_EXECUTION_TRIGGER_KEY) or {}
    public_trigger = signal.get("execution_trigger") or {}
    trigger = signal.get("trigger")
    return bool(
        _execution_source(signal)
        or trigger in {"break_down_confirm", "break_up_confirm"}
        or legacy_trigger.get("trigger") in {"break_down_confirm", "break_up_confirm"}
        or public_trigger.get("trigger") in {"break_down_confirm", "break_up_confirm"}
        or _side_structure_seen(signal, side)
    )


def _block(stage: str, reason: str, source_stage: str) -> dict:
    return {"stage": stage, "blocked": True, "blocked_at": source_stage, "reason": reason}


def _context_dict(type_: str, level, reason: str, timeframe: str = "4h", touches=None, source="ranked_context"):
    lvl = _level(level)
    if lvl is None:
        return None
    out = {"type": type_, "level": lvl, "reason": reason, "timeframe": timeframe, "scope": "macro", "source": source}
    if touches is not None:
        out["touches"] = touches
    return out


def _candidate_from_value(signal: dict, field: str, type_: str, side: str, quality: int, timeframe="4h"):
    level = _level(signal.get(field))
    if level is None:
        return None
    return _context_dict(type_, level, f"{field} ranked as {side} macro context", timeframe, source=field) | {"base_quality": quality}


def _candidate_from_shelf(signal: dict, field: str, side: str, quality: int):
    shelf = signal.get(field) or {}
    level = _level(shelf)
    if level is None:
        return None
    touches = shelf.get("touches")
    q = quality + min(int(touches or 0), 12)
    return _context_dict(shelf.get("type") or field, level, shelf.get("reason") or f"{field} ranked as macro context", shelf.get("timeframe") or "4h", touches, field) | {"base_quality": q}


def _rank_context_candidate(signal: dict, candidate: dict, side: str) -> dict:
    price = _price(signal)
    level = _level(candidate)
    distance_pct = abs(price - level) / price if price > 0 and level is not None else 999.0
    score = float(candidate.get("base_quality") or 0)
    score += max(0.0, 30.0 - distance_pct * 300.0)
    if distance_pct > CONTEXT_TOO_FAR_PCT:
        score -= 45.0
    if side == "bull":
        score += 15.0 if level <= price else -25.0
    elif side == "bear":
        score += 15.0 if level >= price else -12.0
    selected = dict(candidate)
    selected["distance_pct"] = distance_pct
    selected["score"] = round(score, 4)
    selected["actionable"] = bool((side == "bull" and level <= price) or (side == "bear" and level >= price))
    return selected


def _rank_target_candidate(signal: dict, candidate: dict, side: str, selected_context_level) -> dict:
    price = _price(signal)
    level = _level(candidate)
    distance_pct = abs(price - level) / price if price > 0 and level is not None else 999.0
    overlap = bool(price > 0 and selected_context_level is not None and abs(level - selected_context_level) / price <= CONTEXT_TARGET_OVERLAP_PCT)
    directional = bool((side == "bull" and level > price) or (side == "bear" and level < price))
    score = float(candidate.get("base_quality") or 0)
    score += 30.0 if directional else -100.0
    if overlap:
        score -= 80.0
    if distance_pct < MIN_TARGET_DISTANCE_PCT:
        score -= 50.0
    score += max(0.0, 25.0 - distance_pct * 120.0)
    selected = dict(candidate)
    selected["distance_pct"] = distance_pct
    selected["score"] = round(score, 4)
    selected["directional"] = directional
    selected["overlaps_context"] = overlap
    return selected


def _macro_context_candidates(signal: dict, side: str) -> list:
    if side == "bull":
        raw = [
            _candidate_from_value(signal, "range_low_4h", "range_low_4h", side, 84),
            _candidate_from_shelf(signal, "old_support_shelf", side, 76),
            _candidate_from_value(signal, "previous_day_low", "previous_day_low", side, 62),
            _candidate_from_value(signal, "previous_week_low", "previous_week_low", side, 54),
            _candidate_from_value(signal, "major_swing_low_4h", "major_swing_low_4h", side, 48),
        ]
    elif side == "bear":
        raw = [
            _candidate_from_value(signal, "range_high_4h", "range_high_4h", side, 84),
            _candidate_from_shelf(signal, "old_resistance_shelf", side, 76),
            _candidate_from_value(signal, "previous_day_high", "previous_day_high", side, 62),
            _candidate_from_value(signal, "previous_week_high", "previous_week_high", side, 54),
            _candidate_from_value(signal, "major_swing_high_4h", "major_swing_high_4h", side, 48),
        ]
    else:
        raw = []
    ranked = [_rank_context_candidate(signal, c, side) for c in raw if c]
    return sorted(ranked, key=lambda c: c.get("score", -999), reverse=True)


def _target_candidates(signal: dict, side: str, selected_context_level) -> list:
    if side == "bull":
        raw = [
            _candidate_from_shelf(signal, "old_resistance_shelf", side, 78),
            _candidate_from_value(signal, "range_high_4h", "range_high", side, 68),
            _candidate_from_value(signal, "previous_day_high", "previous_day_high", side, 58),
            _candidate_from_value(signal, "previous_week_high", "previous_week_high", side, 52),
            _candidate_from_value(signal, "major_swing_high_4h", "major_swing_high_4h", side, 45),
        ]
    elif side == "bear":
        raw = [
            _candidate_from_shelf(signal, "old_support_shelf", side, 78),
            _candidate_from_value(signal, "range_low_4h", "range_low", side, 68),
            _candidate_from_value(signal, "previous_day_low", "previous_day_low", side, 58),
            _candidate_from_value(signal, "previous_week_low", "previous_week_low", side, 52),
            _candidate_from_value(signal, "major_swing_low_4h", "major_swing_low_4h", side, 45),
        ]
    else:
        raw = []
    ranked = [_rank_target_candidate(signal, c, side, selected_context_level) for c in raw if c]
    return sorted(ranked, key=lambda c: c.get("score", -999), reverse=True)


def _apply_ranked_context_selection(signal: dict) -> None:
    side = _bias_side(signal)
    context_candidates = _macro_context_candidates(signal, side)
    selected_context = context_candidates[0] if context_candidates else None
    selected_context_level = _level(selected_context)
    target_candidates = _target_candidates(signal, side, selected_context_level)
    valid_targets = [c for c in target_candidates if c.get("directional") and not c.get("overlaps_context") and c.get("distance_pct", 999) >= MIN_TARGET_DISTANCE_PCT]
    selected_target = valid_targets[0] if valid_targets else (target_candidates[0] if target_candidates else None)

    signal["context_selection_debug"] = {
        "side": side,
        "selected_macro_context": selected_context,
        "context_candidates": context_candidates[:8],
        "selected_target": selected_target,
        "target_candidates": target_candidates[:8],
        "selection_model": "ranked_4h_context_then_opposite_target_v4_no_zone_gate",
    }

    if selected_context:
        clean_context = {k: v for k, v in selected_context.items() if k not in {"base_quality", "score", "actionable"}}
        clean_context["reason"] = f"ranked 4h macro context: {clean_context.get('reason')}"
        signal["macro_liquidity_context"] = clean_context
        signal["liquidity_context"] = clean_context
        event = signal.get("wyckoff_event_level")
        if isinstance(event, dict) and event.get("valid", True):
            event.update({
                "type": clean_context.get("type"),
                "level": clean_context.get("level"),
                "timeframe": clean_context.get("timeframe", "4h"),
                "side": side,
                "source": "ranked_macro_context",
                "distance_pct": clean_context.get("distance_pct"),
                "reason": "ranked structural Wyckoff event level selected before local entry context",
            })
            signal["wyckoff_event_level"] = event
        req = signal.get("wyckoff_requirement")
        if isinstance(req, dict):
            req["event_level"] = signal.get("wyckoff_event_level") or clean_context
            req["entry_level"] = clean_context.get("level")
            req["distance_pct"] = clean_context.get("distance_pct")
            signal["wyckoff_requirement"] = req

    if selected_target:
        clean_target = {k: v for k, v in selected_target.items() if k not in {"base_quality", "score", "directional", "overlaps_context"}}
        clean_target["timeframe"] = clean_target.get("timeframe") or "4h"
        clean_target["projected"] = True
        clean_target["reason"] = f"ranked projected execution target: {clean_target.get('reason')}"
        signal["execution_target"] = dict(clean_target)
        signal["projected_target"] = dict(clean_target)


def _macro_context(signal: dict) -> dict:
    selected = (signal.get("context_selection_debug") or {}).get("selected_macro_context")
    if _has_level(selected):
        return selected
    ctx = signal.get("macro_liquidity_context")
    if _has_level(ctx):
        return ctx
    ctx = signal.get("liquidity_context")
    if _has_level(ctx):
        return ctx
    return {}


def _target_level(signal: dict):
    selected = (signal.get("context_selection_debug") or {}).get("selected_target")
    return _level(selected) or _level(signal.get("execution_target")) or _level(signal.get("projected_target"))


def _validate_macro_context(signal: dict, side: str) -> dict:
    if side == "neutral":
        return {"valid": False, "stage": "macro_watch", "blocked_at": "macro_4h", "reason": "neutral_bias"}
    price = _price(signal)
    ctx = _macro_context(signal)
    level = _level(ctx)
    if price <= 0 or level is None:
        return {"valid": False, "stage": "context_invalid", "blocked_at": "context_4h", "reason": "missing_macro_context_level"}

    wyckoff = signal.get("wyckoff_requirement") or {}
    event = signal.get("wyckoff_event_level") or wyckoff.get("event_level") or {}
    swept = bool(wyckoff.get("swept") or event.get("swept"))
    confirmed = bool(wyckoff.get("confirmed"))
    reclaimed_or_rejected = bool(event.get("reclaimed") or confirmed)
    distance_pct = abs(price - level) / price if price > 0 else 999.0

    if distance_pct > CONTEXT_TOO_FAR_PCT:
        return {"valid": False, "stage": "context_invalid", "blocked_at": "context_4h", "reason": "macro_context_too_far_from_price"}
    if side == "bear" and price > level and not swept:
        return {"valid": False, "stage": "context_invalid", "blocked_at": "context_4h", "reason": "bear_context_below_price_without_sweep"}
    if side == "bull" and price < level and not swept:
        return {"valid": False, "stage": "context_invalid", "blocked_at": "context_4h", "reason": "bull_context_above_price_without_sweep"}
    if side == "bear" and price > level and swept and not reclaimed_or_rejected:
        return {"valid": True, "stage": "waiting_1h_event", "blocked_at": "decision_1h", "reason": "waiting_bear_rejection_below_macro_context"}
    if side == "bull" and price < level and swept and not reclaimed_or_rejected:
        return {"valid": True, "stage": "waiting_1h_event", "blocked_at": "decision_1h", "reason": "waiting_bull_reclaim_above_macro_context"}
    return {"valid": True, "stage": None, "blocked_at": None, "reason": "macro_context_valid"}


def _context_target_overlap(signal: dict) -> bool:
    price = _price(signal)
    if price <= 0:
        return False
    context_level = _level(_macro_context(signal))
    target = _target_level(signal)
    if context_level is None or target is None:
        return False
    return abs(context_level - target) / price <= CONTEXT_TARGET_OVERLAP_PCT


def _validate_execution_target(signal: dict, side: str) -> dict:
    price = _price(signal)
    target = _target_level(signal)
    if price <= 0 or target is None:
        return {"valid": False, "reason": "missing_execution_target"}
    if side == "bull" and target <= price:
        return {"valid": False, "reason": "bull_target_not_above_price"}
    if side == "bear" and target >= price:
        return {"valid": False, "reason": "bear_target_not_below_price"}
    if abs(target - price) / price < MIN_TARGET_DISTANCE_PCT:
        return {"valid": False, "reason": "target_distance_too_small"}
    return {"valid": True, "reason": "target_valid"}


def _cycle_position_4h(signal: dict) -> dict:
    side = _bias_side(signal)
    macro = signal.get("macro_window_4h") or {}
    price = _price(signal)
    pos = macro.get("range_position")
    target = _target_level(signal)
    near_target = _near(price, target, 0.006)
    near_support = bool(macro.get("near_support_4h"))
    near_resistance = bool(macro.get("near_resistance_4h"))
    context_level = _level(_macro_context(signal))

    if pos is None:
        zone = "unknown"
    elif pos >= 0.65:
        zone = "premium"
    elif pos <= 0.35:
        zone = "discount"
    else:
        zone = "range_body"

    favorable_bear = bool(side == "bear" and (near_resistance or (pos is not None and pos >= 0.60)))
    favorable_bull = bool(side == "bull" and (near_support or (pos is not None and pos <= 0.40)))
    stage = "entry_window" if favorable_bear or favorable_bull else "context_window"
    return {
        "side": side,
        "zone": zone,
        "range_position": pos,
        "near_support_4h": near_support,
        "near_resistance_4h": near_resistance,
        "near_execution_target": near_target,
        "execution_target": target,
        "context_level": context_level,
        "stage": stage,
        "tradability": "entry_allowed_if_1h_event",
        "is_entry_window": True,
        "reason": f"{side}_4h_context_diagnostic_{zone}" if side != "neutral" else "neutral_context_diagnostic",
    }


def _one_hour_decision_ready(signal: dict, side: str) -> bool:
    refinement = signal.get("refinement_context_1h") or {}
    wyckoff = signal.get("wyckoff_requirement") or {}
    model = signal.get("confirmation_model") or {}
    one_hour = signal.get("one_hour_confirmation_debug") or {}
    status = wyckoff.get("status")
    return bool(
        one_hour.get("valid")
        or model.get("confirmed_by_1h")
        or (refinement.get("valid") and refinement.get("side") == side and (wyckoff.get("setup_ready") or wyckoff.get("confirmed")))
        or (status in WYCKOFF_SETUP_READY_STATUSES)
        or wyckoff.get("setup_ready")
        or wyckoff.get("confirmed")
    )


def _resolve_gate(signal: dict) -> dict:
    side = _bias_side(signal)
    wyckoff = signal.get("wyckoff_requirement") or {}
    zone_validity = signal.get("zone_validity") or {}
    macro_context = _validate_macro_context(signal, side)

    macro_ok = bool(side != "neutral" and macro_context.get("valid"))
    liquidity_ok = bool(_has_level(signal.get("macro_liquidity_context")) or _has_level(signal.get("entry_liquidity_context")) or _has_level(signal.get("liquidity_context")))
    one_hour_ready = _one_hour_decision_ready(signal, side)
    target_ok = bool(zone_validity.get("target_ok") or _target_level(signal) is not None)
    target_validation = _validate_execution_target(signal, side)
    execution_seen = _execution_seen(signal)

    if side == "neutral":
        return _block("macro_watch", "neutral_bias", "macro_4h")
    if not macro_ok:
        return _block(macro_context["stage"], macro_context["reason"], macro_context["blocked_at"])
    if macro_context.get("blocked_at") == "decision_1h":
        return _block("waiting_1h_event", macro_context["reason"], "decision_1h")
    if _context_target_overlap(signal):
        return _block("context_target_overlap", "context_target_overlap", "target")
    if not liquidity_ok:
        return _block("liquidity_watch", "missing_liquidity_context", "liquidity_1h")
    if not target_ok:
        return _block("target_watch", "missing_execution_target", "target")
    if not target_validation.get("valid"):
        return _block("target_watch", target_validation["reason"], "target")
    if not one_hour_ready:
        return _block("waiting_1h_event", wyckoff.get("reason") or "waiting_1h_wyckoff_smc_event", "decision_1h")

    # 1H is decision. 15m confirmation is kept as a quality/timing layer, not a hard gate.
    return {
        "stage": "confirm" if execution_seen else "trade_candidate",
        "blocked": False,
        "blocked_at": None,
        "reason": "hierarchy_confirmed_1h_primary" if not execution_seen else "hierarchy_confirmed_1h_plus_15m",
    }


def _normalize_blocked_debug(signal: dict, gate: dict) -> None:
    if not gate.get("blocked"):
        signal["gated_score"] = signal.get("final_score", signal.get("score"))
        return
    wyckoff = signal.get("wyckoff_requirement")
    if isinstance(wyckoff, dict):
        wyckoff.setdefault("legacy_status", wyckoff.get("status"))
        wyckoff.setdefault("legacy_confirmed", wyckoff.get("confirmed"))
        if gate.get("blocked_at") == "decision_1h":
            wyckoff["status"] = "waiting_1h_event"
        else:
            wyckoff["status"] = f"blocked_by_{gate['blocked_at']}"
        wyckoff["confirmed"] = False
        if gate.get("blocked_at") in {"macro_4h", "context_4h", "liquidity_1h", "target"}:
            wyckoff["setup_ready"] = False
        wyckoff["reason"] = gate.get("reason") or wyckoff.get("reason")
        signal["wyckoff_requirement"] = wyckoff

    zone = signal.get("zone_validity")
    if isinstance(zone, dict):
        zone.setdefault("legacy_valid", zone.get("valid"))
        zone.setdefault("legacy_reason", zone.get("reason"))
        if gate.get("blocked_at") in {"macro_4h", "context_4h", "liquidity_1h", "target"}:
            zone["valid"] = False
            zone["reason"] = gate.get("reason") or zone.get("reason")
        signal["zone_validity"] = zone

    if gate.get("blocked_at") in {"macro_4h", "context_4h", "target"}:
        signal["gated_score"] = 0
    else:
        signal["gated_score"] = signal.get("final_score", signal.get("score"))


def apply_hierarchical_stage_gates(signal: dict) -> dict:
    if not isinstance(signal, dict):
        return signal

    _apply_ranked_context_selection(signal)
    pipeline = dict(signal.get("pipeline") or {})
    cycle = _cycle_position_4h(signal)
    signal["cycle_position_4h"] = cycle
    gate = _resolve_gate(signal)
    accepted = not gate["blocked"]
    side = _bias_side(signal)
    execution_seen = _execution_seen(signal)
    execution_source = _execution_source(signal)

    original_trade = signal.get("trade") or {}
    original_trigger = signal.get("trigger")
    original_confirm_source = _clean_trigger_source(signal.get("confirm_source"))

    _normalize_blocked_debug(signal, gate)

    execution_trigger = dict(signal.get(LEGACY_EXECUTION_TRIGGER_KEY) or signal.get("execution_trigger") or {})
    execution_trigger.update({
        "seen": execution_seen,
        "valid": bool(accepted and execution_seen),
        "accepted": accepted,
        "timeframe": EXECUTION_TIMEFRAME,
        "trigger": original_trigger,
        "confirm_source": execution_source,
        "blocked": not accepted,
        "blocked_by": gate["blocked_at"],
        "block_reason": None if accepted else gate["reason"],
    })

    signal[LEGACY_EXECUTION_TRIGGER_KEY] = execution_trigger
    signal["execution_trigger"] = dict(execution_trigger)
    signal["stage"] = gate["stage"]
    signal["hierarchy_gate"] = {
        "model": "4h_context__ranked_context__1h_decision__15m_optional_v4",
        "side": side,
        "accepted": accepted,
        "stage": gate["stage"],
        "blocked_at": gate["blocked_at"],
        "block_reason": None if accepted else gate["reason"],
        "macro_4h_ok": side != "neutral" and gate["blocked_at"] not in {"macro_4h", "context_4h"},
        "cycle_4h_ok": True,
        "liquidity_ok": gate["stage"] not in PRE_ZONE_STAGES,
        "one_hour_decision_ok": accepted or gate["blocked_at"] not in {"decision_1h"},
        "zone_1h_ok": None,
        "confirm_15m_seen": execution_seen,
        "confirm_15m_accepted": bool(accepted and execution_seen),
        "confirmation_path": "1h_plus_15m" if accepted and execution_seen else "1h_primary_15m_optional" if accepted else "waiting_1h_event",
    }
    signal["confirm_blocked_by_hierarchy"] = not accepted
    signal["confirm_block_reason"] = None if accepted else gate["reason"]
    signal["hierarchy_block_reason"] = None if accepted else gate["reason"]

    pipeline["collect"] = True
    pipeline["liquidity"] = gate["stage"] not in PRE_LIQUIDITY_STAGES
    pipeline["zone"] = gate["stage"] not in PRE_ZONE_STAGES
    pipeline["confirm"] = accepted
    pipeline["trade"] = bool(pipeline.get("trade") and accepted)
    signal["pipeline"] = pipeline

    if accepted:
        signal["confirm_source"] = execution_source or original_confirm_source or signal.get("confirm_source")
        signal["trigger"] = original_trigger if execution_seen else "1h_confirm_15m_optional"
        if gate["stage"] == "trade_candidate":
            signal["planner_candidate_status"] = signal.get("planner_candidate_status") or "candidate_watch"
            signal["planner_candidate_reason"] = None
        if original_trade:
            signal["trade"] = original_trade
    else:
        signal["confirm_source"] = None
        signal["trigger"] = "wait"
        signal["bias"] = "bear_watch" if side == "bear" else "bull_watch" if side == "bull" else "neutral"
        signal["trade"] = {"status": "watch", "side": "none", "entry": None, "stop": None, "target": None}
        signal["planner_candidate_status"] = "not_created"
        signal["planner_candidate_reason"] = f"waiting:{gate['reason']}" if gate["blocked_at"] == "decision_1h" else f"blocked_before_planner:{gate['reason']}"
        signal["planner_candidate_rr"] = None

    return signal

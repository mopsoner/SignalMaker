"""Hierarchical SignalMaker gates for Wyckoff + SMC progression.

This module intentionally sits above the legacy engine. The legacy engine can still
collect context and detect local structure, but it cannot promote a signal to
`confirm` unless the higher-timeframe gates agree:

4H  -> macro campaign context / cycle location
1H  -> liquidity / Wyckoff-SMC setup zone
15M -> execution confirmation only
"""

EXECUTION_TIMEFRAME = "15m"
LEGACY_EXECUTION_TRIGGER_KEY = "execution_trigger_5m"
MIN_TARGET_DISTANCE_PCT = 0.008
CONTEXT_TARGET_OVERLAP_PCT = 0.003

WYCKOFF_EXECUTION_READY_STATUSES = {
    "execution_ready",
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
    "mid_cycle_watch",
    "late_bear_cycle",
    "late_bull_cycle",
    "context_invalid",
    "context_target_overlap",
    "entry_context_watch",
    "target_watch",
}

PRE_ZONE_STAGES = PRE_LIQUIDITY_STAGES | {"liquidity_watch"}
PRE_CONFIRM_STAGES = PRE_ZONE_STAGES | {"zone_watch", "wyckoff_watch"}


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
    if not isinstance(value, dict):
        return None
    raw = value.get("level")
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


def _execution_seen(signal: dict) -> bool:
    """True only when a real local execution event was detected."""
    side = _bias_side(signal)
    legacy_trigger = signal.get(LEGACY_EXECUTION_TRIGGER_KEY) or {}
    public_trigger = signal.get("execution_trigger") or {}
    source = _execution_source(signal)
    trigger = signal.get("trigger")
    return bool(
        source
        or trigger in {"break_down_confirm", "break_up_confirm"}
        or legacy_trigger.get("trigger") in {"break_down_confirm", "break_up_confirm"}
        or public_trigger.get("trigger") in {"break_down_confirm", "break_up_confirm"}
        or _side_structure_seen(signal, side)
    )


def _execution_source(signal: dict):
    legacy_trigger = signal.get(LEGACY_EXECUTION_TRIGGER_KEY) or {}
    public_trigger = signal.get("execution_trigger") or {}
    return _clean_trigger_source(
        legacy_trigger.get("confirm_source")
        or public_trigger.get("confirm_source")
        or signal.get("confirm_source")
    )


def _block(stage: str, reason: str, source_stage: str) -> dict:
    return {
        "stage": stage,
        "blocked": True,
        "blocked_at": source_stage,
        "reason": reason,
    }


def _macro_context(signal: dict) -> dict:
    ctx = signal.get("macro_liquidity_context")
    if _has_level(ctx):
        return ctx
    ctx = signal.get("liquidity_context")
    if _has_level(ctx):
        return ctx
    return {}


def _target_level(signal: dict):
    return _level(signal.get("execution_target")) or _level(signal.get("projected_target"))


def _validate_macro_context(signal: dict, side: str) -> dict:
    """Validate that the selected 4H context is still actionable.

    This does not try to re-rank all candidates. It only prevents the selected
    HTF level from promoting the signal when price is already beyond it and the
    expected Wyckoff rejection/reclaim has not happened yet.
    """
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

    if side == "bear" and price > level:
        if not swept:
            return {"valid": False, "stage": "context_invalid", "blocked_at": "context_4h", "reason": "bear_context_below_price_without_sweep"}
        if not reclaimed_or_rejected:
            return {"valid": False, "stage": "entry_context_watch", "blocked_at": "wyckoff_1h", "reason": "waiting_bear_rejection_below_macro_context"}

    if side == "bull" and price < level:
        if not swept:
            return {"valid": False, "stage": "context_invalid", "blocked_at": "context_4h", "reason": "bull_context_above_price_without_sweep"}
        if not reclaimed_or_rejected:
            return {"valid": False, "stage": "entry_context_watch", "blocked_at": "wyckoff_1h", "reason": "waiting_bull_reclaim_above_macro_context"}

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
    """Classify whether a local 15M trigger is early, mid, or late in the 4H cycle.

    This separates move detection from entry quality. A BOS/MSS into the opposite
    4H side is not a new entry; it is a mature move / TP area.
    """
    side = _bias_side(signal)
    macro = signal.get("macro_window_4h") or {}
    price = float(signal.get("price") or 0.0)
    pos = macro.get("range_position")
    target = (signal.get("execution_target") or {}).get("level") or (signal.get("projected_target") or {}).get("level")
    near_target = _near(price, target, 0.006)
    near_support = bool(macro.get("near_support_4h"))
    near_resistance = bool(macro.get("near_resistance_4h"))

    if pos is None:
        zone = "unknown"
    elif pos >= 0.65:
        zone = "premium"
    elif pos <= 0.35:
        zone = "discount"
    else:
        zone = "mid_range"

    late_bear = bool(side == "bear" and (near_target or near_support or (pos is not None and pos <= 0.40)))
    late_bull = bool(side == "bull" and (near_target or near_resistance or (pos is not None and pos >= 0.60)))
    favorable_bear = bool(side == "bear" and (near_resistance or (pos is not None and pos >= 0.60)))
    favorable_bull = bool(side == "bull" and (near_support or (pos is not None and pos <= 0.40)))

    if late_bear:
        stage = "late_bear_cycle"
        tradability = "no_new_short"
        reason = "late_bear_cycle_near_support_or_target"
    elif late_bull:
        stage = "late_bull_cycle"
        tradability = "no_new_long"
        reason = "late_bull_cycle_near_resistance_or_target"
    elif favorable_bear or favorable_bull:
        stage = "entry_window"
        tradability = "entry_allowed_if_1h_15m_confirm"
        reason = f"{side}_entry_window_{zone}"
    else:
        stage = "mid_cycle"
        tradability = "wait_for_edge"
        reason = f"{side}_mid_cycle_no_clear_edge"

    return {
        "side": side,
        "zone": zone,
        "range_position": pos,
        "near_support_4h": near_support,
        "near_resistance_4h": near_resistance,
        "near_execution_target": near_target,
        "execution_target": target,
        "stage": stage,
        "tradability": tradability,
        "is_late_cycle": late_bear or late_bull,
        "is_entry_window": favorable_bear or favorable_bull,
        "reason": reason,
    }


def _resolve_gate(signal: dict) -> dict:
    side = _bias_side(signal)
    macro = signal.get("macro_window_4h") or {}
    refinement = signal.get("refinement_context_1h") or {}
    wyckoff = signal.get("wyckoff_requirement") or {}
    zone_validity = signal.get("zone_validity") or {}
    cycle = signal.get("cycle_position_4h") or _cycle_position_4h(signal)
    macro_context = _validate_macro_context(signal, side)

    # Late cycle has priority: the move can be real, but the entry is too late.
    if cycle.get("is_late_cycle"):
        return _block(cycle["stage"], cycle["reason"], "cycle_4h")

    macro_ok = bool(macro.get("valid") and side != "neutral" and macro.get("side") == side)
    liquidity_ok = bool(
        _has_level(signal.get("macro_liquidity_context"))
        or _has_level(signal.get("entry_liquidity_context"))
        or _has_level(signal.get("liquidity_context"))
    )
    zone_1h_ok = bool(refinement.get("valid") and refinement.get("side") == side)

    wyckoff_needed = bool(wyckoff.get("needed"))
    wyckoff_status = wyckoff.get("status")
    wyckoff_setup_ok = bool(
        not wyckoff_needed
        or wyckoff.get("setup_ready")
        or wyckoff.get("confirmed")
        or wyckoff_status in WYCKOFF_SETUP_READY_STATUSES
    )
    wyckoff_execution_ok = bool(
        not wyckoff_needed
        or wyckoff.get("confirmed")
        or wyckoff_status in WYCKOFF_EXECUTION_READY_STATUSES
    )
    target_ok = bool(zone_validity.get("target_ok") or (signal.get("execution_target") or {}).get("level") is not None or (signal.get("projected_target") or {}).get("level") is not None)
    target_validation = _validate_execution_target(signal, side)
    execution_seen = _execution_seen(signal)

    if side == "neutral":
        return _block("macro_watch", "neutral_bias", "macro_4h")
    if not macro_context.get("valid"):
        return _block(macro_context["stage"], macro_context["reason"], macro_context["blocked_at"])
    if _context_target_overlap(signal):
        return _block("context_target_overlap", "context_target_overlap", "target")
    if not macro_ok:
        if cycle.get("stage") == "mid_cycle":
            return _block("mid_cycle_watch", cycle.get("reason") or "mid_cycle_no_clear_4h_edge", "cycle_4h")
        return _block("macro_watch", f"missing_4h_{side}_window:{macro.get('reason') or 'no_clear_4h_trade_window'}", "macro_4h")
    if not liquidity_ok:
        return _block("liquidity_watch", "missing_liquidity_context", "liquidity_1h")
    if not zone_1h_ok:
        return _block("zone_watch", refinement.get("reason") or f"no_1h_{side}_setup", "zone_1h")
    if not wyckoff_setup_ok:
        return _block("wyckoff_watch", wyckoff.get("reason") or "wyckoff_setup_not_ready", "wyckoff_1h")
    if not target_ok:
        return _block("zone_watch", "missing_execution_target", "zone_1h")
    if not target_validation.get("valid"):
        return _block("target_watch", target_validation["reason"], "target")
    if not execution_seen:
        return _block("confirm_watch", f"waiting_{EXECUTION_TIMEFRAME}_confirm", "confirm_15m")
    if not wyckoff_execution_ok:
        return _block("confirm_watch", wyckoff.get("reason") or "wyckoff_waiting_rejection_or_reclaim", "confirm_15m")

    return {
        "stage": "confirm",
        "blocked": False,
        "blocked_at": None,
        "reason": "hierarchy_confirmed",
    }


def _normalize_blocked_debug(signal: dict, gate: dict) -> None:
    """Avoid contradictory debug when a lower layer saw execution but HTF gates reject it."""
    if not gate.get("blocked"):
        return

    wyckoff = signal.get("wyckoff_requirement")
    if isinstance(wyckoff, dict):
        wyckoff.setdefault("legacy_status", wyckoff.get("status"))
        wyckoff.setdefault("legacy_confirmed", wyckoff.get("confirmed"))
        wyckoff["status"] = f"blocked_by_{gate['blocked_at']}"
        wyckoff["confirmed"] = False
        if gate.get("blocked_at") in {"macro_4h", "context_4h", "liquidity_1h", "zone_1h", "target", "cycle_4h"}:
            wyckoff["setup_ready"] = False
        wyckoff["reason"] = gate.get("reason") or wyckoff.get("reason")
        signal["wyckoff_requirement"] = wyckoff

    zone = signal.get("zone_validity")
    if isinstance(zone, dict):
        zone.setdefault("legacy_valid", zone.get("valid"))
        zone.setdefault("legacy_reason", zone.get("reason"))
        if gate.get("blocked_at") in {"macro_4h", "context_4h", "liquidity_1h", "zone_1h", "wyckoff_1h", "target", "cycle_4h"}:
            zone["valid"] = False
            zone["reason"] = gate.get("reason") or zone.get("reason")
        signal["zone_validity"] = zone

    if gate.get("blocked_at") in {"macro_4h", "context_4h", "target", "cycle_4h"}:
        signal["gated_score"] = 0
    else:
        signal["gated_score"] = signal.get("final_score", signal.get("score"))


def apply_hierarchical_stage_gates(signal: dict) -> dict:
    """Mutate and return a signal using strict HTF -> execution gates."""
    if not isinstance(signal, dict):
        return signal

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
        "valid": accepted,
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
        "model": "4h_cycle__4h_macro__1h_zone__execution_confirm",
        "side": side,
        "accepted": accepted,
        "stage": gate["stage"],
        "blocked_at": gate["blocked_at"],
        "block_reason": None if accepted else gate["reason"],
        "macro_4h_ok": gate["stage"] not in PRE_LIQUIDITY_STAGES and side != "neutral",
        "cycle_4h_ok": not cycle.get("is_late_cycle") and cycle.get("stage") != "mid_cycle",
        "liquidity_ok": gate["stage"] not in PRE_ZONE_STAGES,
        "zone_1h_ok": gate["stage"] not in PRE_CONFIRM_STAGES,
        "confirm_15m_seen": execution_seen,
        "confirm_15m_accepted": accepted,
    }
    signal["confirm_blocked_by_hierarchy"] = not accepted
    signal["confirm_block_reason"] = None if accepted else gate["reason"]
    signal["hierarchy_block_reason"] = None if accepted else gate["reason"]

    pipeline["collect"] = True
    pipeline["liquidity"] = gate["stage"] not in PRE_LIQUIDITY_STAGES
    pipeline["zone"] = gate["stage"] not in PRE_CONFIRM_STAGES
    pipeline["confirm"] = accepted
    pipeline["trade"] = bool(pipeline.get("trade") and accepted)
    signal["pipeline"] = pipeline

    if accepted:
        signal["confirm_source"] = execution_source or original_confirm_source
        signal["trigger"] = original_trigger
        if original_trade:
            signal["trade"] = original_trade
    else:
        signal["confirm_source"] = None
        signal["trigger"] = "wait"
        if side == "bear":
            signal["bias"] = "bear_watch"
        elif side == "bull":
            signal["bias"] = "bull_watch"
        else:
            signal["bias"] = "neutral"
        signal["trade"] = {
            "status": "watch",
            "side": "none",
            "entry": None,
            "stop": None,
            "target": None,
        }
        signal["planner_candidate_status"] = "not_created"
        signal["planner_candidate_reason"] = f"blocked_before_planner:{gate['reason']}"
        signal["planner_candidate_rr"] = None

    return signal

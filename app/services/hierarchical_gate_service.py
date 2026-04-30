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


def _bias_side(signal: dict) -> str:
    bias = str(signal.get("bias") or "neutral")
    if bias.startswith("bear"):
        return "bear"
    if bias.startswith("bull"):
        return "bull"
    return "neutral"


def _has_level(value) -> bool:
    return isinstance(value, dict) and value.get("level") is not None


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
    execution_seen = _execution_seen(signal)

    if side == "neutral":
        return _block("macro_watch", "neutral_bias", "macro_4h")
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
        if gate.get("blocked_at") in {"macro_4h", "liquidity_1h", "zone_1h", "cycle_4h"}:
            wyckoff["setup_ready"] = False
        wyckoff["reason"] = gate.get("reason") or wyckoff.get("reason")
        signal["wyckoff_requirement"] = wyckoff

    zone = signal.get("zone_validity")
    if isinstance(zone, dict):
        zone.setdefault("legacy_valid", zone.get("valid"))
        zone.setdefault("legacy_reason", zone.get("reason"))
        if gate.get("blocked_at") in {"macro_4h", "liquidity_1h", "zone_1h", "wyckoff_1h", "cycle_4h"}:
            zone["valid"] = False
            zone["reason"] = gate.get("reason") or zone.get("reason")
        signal["zone_validity"] = zone

    if gate.get("blocked_at") in {"macro_4h", "cycle_4h"}:
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
        "macro_4h_ok": gate["stage"] not in {"macro_watch", "mid_cycle_watch", "late_bear_cycle", "late_bull_cycle"} and side != "neutral",
        "cycle_4h_ok": not cycle.get("is_late_cycle") and cycle.get("stage") != "mid_cycle",
        "liquidity_ok": gate["stage"] not in {"macro_watch", "mid_cycle_watch", "late_bear_cycle", "late_bull_cycle", "liquidity_watch"},
        "zone_1h_ok": gate["stage"] not in {"macro_watch", "mid_cycle_watch", "late_bear_cycle", "late_bull_cycle", "liquidity_watch", "zone_watch"},
        "confirm_15m_seen": execution_seen,
        "confirm_15m_accepted": accepted,
    }
    signal["confirm_blocked_by_hierarchy"] = not accepted
    signal["confirm_block_reason"] = None if accepted else gate["reason"]
    signal["hierarchy_block_reason"] = None if accepted else gate["reason"]

    pipeline["collect"] = True
    pipeline["liquidity"] = gate["stage"] not in {"macro_watch", "mid_cycle_watch", "late_bear_cycle", "late_bull_cycle"}
    pipeline["zone"] = gate["stage"] not in {"macro_watch", "mid_cycle_watch", "late_bear_cycle", "late_bull_cycle", "liquidity_watch", "zone_watch", "wyckoff_watch"}
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

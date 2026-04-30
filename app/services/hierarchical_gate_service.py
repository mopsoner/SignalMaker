"""Hierarchical SignalMaker gates for Wyckoff + SMC progression.

This module intentionally sits above the legacy engine. The legacy engine can still
collect context and detect local structure, but it cannot promote a signal to
`confirm` unless the higher-timeframe gates agree:

4H  -> macro campaign context
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
    if source == "5m_bos":
        return "15m_bos"
    return source


def _execution_seen(signal: dict) -> bool:
    pipeline = signal.get("pipeline") or {}
    legacy_trigger = signal.get(LEGACY_EXECUTION_TRIGGER_KEY) or {}
    public_trigger = signal.get("execution_trigger") or {}
    return bool(
        pipeline.get("confirm")
        or legacy_trigger.get("valid")
        or public_trigger.get("valid")
        or signal.get("confirm_source")
        or signal.get("trigger") in {"break_down_confirm", "break_up_confirm"}
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


def _resolve_gate(signal: dict) -> dict:
    side = _bias_side(signal)
    macro = signal.get("macro_window_4h") or {}
    refinement = signal.get("refinement_context_1h") or {}
    wyckoff = signal.get("wyckoff_requirement") or {}
    zone_validity = signal.get("zone_validity") or {}

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


def apply_hierarchical_stage_gates(signal: dict) -> dict:
    """Mutate and return a signal using strict HTF -> execution gates.

    The important distinction is:
    - execution trigger seen: local 15M structure was detected
    - execution trigger accepted: 4H + 1H + Wyckoff gates authorize it
    """
    if not isinstance(signal, dict):
        return signal

    pipeline = dict(signal.get("pipeline") or {})
    gate = _resolve_gate(signal)
    accepted = not gate["blocked"]
    side = _bias_side(signal)
    execution_seen = _execution_seen(signal)
    execution_source = _execution_source(signal)

    original_trade = signal.get("trade") or {}
    original_trigger = signal.get("trigger")
    original_confirm_source = _clean_trigger_source(signal.get("confirm_source"))

    # Preserve local structure diagnostics, but separate detected vs authorized.
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
        "model": "4h_macro__1h_zone__15m_confirm",
        "side": side,
        "accepted": accepted,
        "stage": gate["stage"],
        "blocked_at": gate["blocked_at"],
        "block_reason": None if accepted else gate["reason"],
        "macro_4h_ok": gate["stage"] not in {"macro_watch"} and side != "neutral",
        "liquidity_ok": gate["stage"] not in {"macro_watch", "liquidity_watch"},
        "zone_1h_ok": gate["stage"] not in {"macro_watch", "liquidity_watch", "zone_watch"},
        "confirm_15m_seen": execution_seen,
        "confirm_15m_accepted": accepted,
    }
    signal["confirm_blocked_by_hierarchy"] = not accepted
    signal["confirm_block_reason"] = None if accepted else gate["reason"]
    signal["hierarchy_block_reason"] = None if accepted else gate["reason"]

    pipeline["collect"] = True
    pipeline["liquidity"] = gate["stage"] not in {"macro_watch"}
    pipeline["zone"] = gate["stage"] not in {"macro_watch", "liquidity_watch", "zone_watch", "wyckoff_watch"}
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

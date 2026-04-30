"""Context-driven signal progression helpers.

The 4H macro window is diagnostic/scoring context. Once the engine has already
identified macro liquidity, entry liquidity and a target, the setup can progress
to Zone/Confirm based on those contexts and the local execution trigger.
"""

from __future__ import annotations


def _has_context(value: dict | None) -> bool:
    if not isinstance(value, dict):
        return False
    if value.get("type") in (None, "", "none"):
        return False
    return value.get("level") is not None


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

    if context_ok:
        pipeline["collect"] = True
        pipeline["liquidity"] = True
        pipeline["zone"] = True
        if setup_ready or confirm_ok:
            zone_validity = signal.setdefault("zone_validity", {})
            zone_validity["valid"] = True
            zone_validity["target_ok"] = True
            zone_validity["wyckoff_ok"] = bool(setup_ready or zone_validity.get("wyckoff_ok"))
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
            wyckoff["status"] = "execution_ready" if confirm_ok else "context_ready"
            wyckoff["reason"] = "context identified; 4h window kept as diagnostic only"

    return signal

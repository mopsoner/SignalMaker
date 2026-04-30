"""Service package bootstrap.

Small runtime patch for the legacy hierarchical signal engine.

The legacy engine still computes useful 4H diagnostics, but the 4H window must
not hard-block the setup progression once liquidity, entry and target contexts
are identified. This patch keeps 4H as scoring/diagnostic metadata and lets the
pipeline progress to Zone/Confirm based on actual context + execution trigger.
"""

from __future__ import annotations


def _install_context_driven_hierarchy_patch() -> None:
    try:
        from app.services import signal_engine_service as engine_module
    except Exception:
        return

    cls = getattr(engine_module, "SignalEngineService", None)
    if cls is None or getattr(cls, "_context_driven_hierarchy_patched", False):
        return

    original_apply_hierarchy = cls._apply_hierarchy

    def _has_context(value: dict | None) -> bool:
        if not isinstance(value, dict):
            return False
        if value.get("type") in (None, "", "none"):
            return False
        return value.get("level") is not None

    def _patched_apply_hierarchy(self, signal: dict, candles_1h: list[dict], candles_5m: list[dict], cfg: dict) -> dict:
        result = original_apply_hierarchy(self, signal, candles_1h, candles_5m, cfg)
        pipeline = result.setdefault("pipeline", {})
        macro_ctx = result.get("macro_liquidity_context") or result.get("liquidity_context") or {}
        entry_ctx = result.get("entry_liquidity_context") or {}
        target = result.get("execution_target") or result.get("projected_target") or {}
        wyckoff = result.get("wyckoff_requirement") or {}
        exec_trigger = result.get("execution_trigger_5m") or result.get("execution_trigger") or {}
        block_reason = result.get("hierarchy_block_reason")

        context_ok = _has_context(macro_ctx) and _has_context(entry_ctx) and _has_context(target)
        setup_ready = bool(wyckoff.get("setup_ready") or wyckoff.get("confirmed"))
        confirm_ok = bool(exec_trigger.get("valid"))

        if context_ok:
            pipeline["collect"] = True
            pipeline["liquidity"] = True
            pipeline["zone"] = True
            if setup_ready or confirm_ok:
                zone_validity = result.setdefault("zone_validity", {})
                zone_validity["valid"] = True
                zone_validity["target_ok"] = True
                zone_validity["wyckoff_ok"] = bool(setup_ready or zone_validity.get("wyckoff_ok"))
                zone_validity["reason"] = "valid_context_zone"
                result["zone_quality"] = result.get("zone_quality") if result.get("zone_quality") != "weak" else "medium"

        if context_ok and confirm_ok:
            pipeline["confirm"] = True
            result["trigger"] = exec_trigger.get("trigger") or result.get("trigger")

        if context_ok and block_reason in {"blocked_no_4h_bull_window", "blocked_no_4h_bear_window"}:
            result["hierarchy_block_reason"] = None
            if result.get("planner_candidate_reason") == block_reason:
                result["planner_candidate_reason"] = None
            if isinstance(wyckoff, dict) and wyckoff.get("status") == "blocked":
                wyckoff["status"] = "execution_ready" if confirm_ok else "context_ready"
                wyckoff["reason"] = "context identified; 4h window kept as diagnostic only"

        return result

    cls._apply_hierarchy = _patched_apply_hierarchy
    cls._context_driven_hierarchy_patched = True


_install_context_driven_hierarchy_patch()

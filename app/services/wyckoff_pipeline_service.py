"""Source-agnostic entry point for the Wyckoff/SMC decision pipeline."""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

from app.services.hierarchical_gate_service import apply_hierarchical_stage_gates
from app.services.planner_service import PlannerService
from app.services.signal_context_service import apply_context_driven_progression
from app.services.signal_engine_service import SignalEngineService


class WyckoffPipelineService:
    """Run normalized candles through the shared engine, contexts, gates and planner."""

    def __init__(self, *, engine=None, planner=None, score_signal: Callable[[dict], dict] | None = None):
        self.engine = engine or SignalEngineService()
        self.planner = planner or PlannerService()
        self.score_signal = score_signal or (lambda signal: signal)

    @staticmethod
    def public_signal(signal: dict, execution_interval: str = "15m") -> dict:
        payload = dict(signal)
        trigger = payload.pop("execution_trigger_5m", None)
        if trigger and "execution_trigger" not in payload:
            payload["execution_trigger"] = trigger
        if isinstance(payload.get("execution_trigger"), dict):
            payload["execution_trigger"]["timeframe"] = execution_interval
        payload.pop("one_hour_confirmation_debug", None)
        payload.pop("rsi_5m", None)
        payload["rsi_15m"] = payload.get("rsi_main")
        payload["rsi_main_timeframe"] = execution_interval
        payload["signal_interval"] = execution_interval
        payload["execution_timeframe"] = execution_interval
        return payload

    @staticmethod
    def _enforce_one_hour_gate(signal: dict) -> dict:
        decision = signal.get("one_hour_decision") or {}
        if decision.get("valid"):
            return signal
        reason = decision.get("reason") or "waiting_1h_decision"
        signal.setdefault("pipeline", {}).update(collect=True, liquidity=True, zone=True, confirm=False, trade=False)
        signal.update(stage="waiting_1h_event", state="waiting_1h_event", trigger="wait", confirm_source=None,
                      trade={"status": "watch", "side": "none", "entry": None, "stop": None, "target": None},
                      planner_candidate_status="not_created", planner_candidate_reason=f"blocked_before_planner:{reason}",
                      planner_candidate_rr=None, hierarchy_block_reason=reason, confirm_blocked_by_hierarchy=True,
                      confirm_block_reason=reason)
        wyckoff = signal.get("wyckoff_requirement")
        if isinstance(wyckoff, dict):
            wyckoff.update(status="waiting_1h_event", confirmed=False, setup_ready=False, reason=reason)
        signal.setdefault("confirmation_model", {}).update(confirmed_by_1h=False, entry_mode="wait", confirmation_source=decision.get("source"))
        trigger = signal.get("execution_trigger")
        if isinstance(trigger, dict):
            trigger.update(valid=False, accepted=False, blocked=True, blocked_by="decision_1h", block_reason=reason)
        signal.setdefault("hierarchy_gate", {}).update(
            accepted=False, stage="waiting_1h_event", blocked_at="decision_1h", block_reason=reason,
            one_hour_decision_ok=False, confirm_15m_accepted=False, confirmation_path="waiting_1h_event")
        return signal

    def analyze(self, *, symbol: str, candles: dict[str, list[dict]], market_context: dict[str, Any] | None = None,
                execution_interval: str = "15m") -> tuple[dict, dict]:
        missing = [tf for tf in (execution_interval, "1h", "4h") if not candles.get(tf)]
        if missing:
            raise ValueError(f"missing_required_timeframes:{','.join(missing)}")
        bundle = dict(candles)
        bundle["5m"] = bundle[execution_interval]
        signal = self.engine.compute_signal(symbol, bundle)
        signal = apply_context_driven_progression(signal)
        signal.update(execution_timeframe=execution_interval, signal_interval=execution_interval,
                      rsi_main_timeframe=execution_interval)
        if signal.get("execution_trigger_5m"):
            signal["execution_trigger"] = {**signal["execution_trigger_5m"], "timeframe": execution_interval}
        signal = apply_hierarchical_stage_gates(signal)
        signal = apply_context_driven_progression(signal)
        signal = self._enforce_one_hour_gate(signal)
        signal = self.score_signal(signal)
        if signal.get("confirm_blocked_by_hierarchy"):
            assessment = {"accepted": False, "reason": signal.get("planner_candidate_reason") or signal.get("confirm_block_reason"), "rr_ratio": None, "candidate": None}
        else:
            assessment = self.planner.assess_signal(signal)
            signal["planner_candidate_status"] = "open_candidate" if assessment["accepted"] else "rejected"
            signal["planner_candidate_reason"] = assessment["reason"]
            signal["planner_candidate_rr"] = assessment.get("rr_ratio")
        signal["market_context"] = dict(market_context or {})
        signal["available_timeframes"] = sorted(candles)
        return self.public_signal(signal, execution_interval), assessment

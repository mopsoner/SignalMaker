from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.models.trade_candidate import TradeCandidate
from app.schemas.momentum import ALLOWED_MOMENTUM_DECISION_ACTIONS, MomentumDecision
from app.services.executor_service import ExecutorService
from app.services.trade_candidate_service import TradeCandidateService


class MomentumDecisionService:
    """Build and execute the dashboard/TUI momentum decision contract."""

    def __init__(self, db: Session) -> None:
        self.db = db
        self.candidates = TradeCandidateService(db)

    def decision(self) -> dict[str, Any]:
        candidates = self.candidates.list_candidates(
            limit=1,
            status="open",
            stage="momentum",
            exclude_test_data=True,
        )
        if not candidates:
            return self._idle_decision()
        return self._candidate_decision(candidates[0])

    def run_once(self, quantity: float = 1.0, mode: str = "paper") -> dict[str, Any]:
        result = ExecutorService(self.db).execute_momentum_decision(
            quantity=quantity,
            mode=mode,
        )
        decision = self._display_decision_from_execution(result)
        return {"decision": decision, "result": result}

    def _contract(self, **payload: Any) -> dict[str, Any]:
        return MomentumDecision(**payload).model_dump()

    def _idle_decision(self) -> dict[str, Any]:
        return self._contract(
            decision_action="HOLD",
            symbol=None,
            target_symbol=None,
            status="idle",
            reason="no_open_momentum_candidate",
            order_ids=[],
            fill_ids=[],
        )

    def _candidate_action(self, candidate: TradeCandidate) -> str:
        action = "BUY" if candidate.side == "long" else str(candidate.side or "").upper()
        return action if action in ALLOWED_MOMENTUM_DECISION_ACTIONS else "HOLD"

    def _candidate_decision(self, candidate: TradeCandidate) -> dict[str, Any]:
        return self._contract(
            decision_action=self._candidate_action(candidate),
            symbol=candidate.symbol,
            target_symbol=candidate.symbol,
            status="ready",
            reason="open_momentum_candidate",
            order_ids=[],
            fill_ids=[],
            candidate_id=candidate.candidate_id,
            side=candidate.side,
            score=candidate.score,
            entry_price=candidate.entry_price,
            target_price=candidate.target_price,
            stop_price=candidate.stop_price,
        )

    def _display_decision_from_execution(self, result: dict[str, Any]) -> dict[str, Any]:
        action = str(result.get("decision_action") or "HOLD").upper()
        if action not in ALLOWED_MOMENTUM_DECISION_ACTIONS:
            action = "HOLD"
        status = result.get("status") if result.get("status") in {"ready", "idle", "waiting", "skipped", "executed", "error"} else "error"
        return self._contract(
            decision_action=action,
            symbol=result.get("symbol"),
            target_symbol=result.get("target_symbol") or result.get("symbol"),
            status=status,
            reason=result.get("reason", "momentum_execution_completed"),
            order_ids=result.get("order_ids") or [],
            fill_ids=result.get("fill_ids") or [],
            mark_price=result.get("mark_price"),
            target_price=result.get("target_price"),
        )

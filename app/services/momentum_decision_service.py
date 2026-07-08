from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.models.trade_candidate import TradeCandidate
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

    def _idle_decision(self) -> dict[str, Any]:
        return {
            "decision_action": "HOLD",
            "symbol": None,
            "target_symbol": None,
            "status": "idle",
            "reason": "no_open_momentum_candidate",
            "order_ids": [],
            "fill_ids": [],
        }

    def _candidate_decision(self, candidate: TradeCandidate) -> dict[str, Any]:
        action = "BUY" if candidate.side == "long" else str(candidate.side or "").upper()
        return {
            "decision_action": action or "HOLD",
            "symbol": candidate.symbol,
            "target_symbol": candidate.symbol,
            "status": "ready",
            "reason": "open_momentum_candidate",
            "order_ids": [],
            "fill_ids": [],
            "candidate_id": candidate.candidate_id,
            "side": candidate.side,
            "score": candidate.score,
            "entry_price": candidate.entry_price,
            "target_price": candidate.target_price,
            "stop_price": candidate.stop_price,
        }

    def _display_decision_from_execution(self, result: dict[str, Any]) -> dict[str, Any]:
        decision = {
            "decision_action": result.get("decision_action", "HOLD"),
            "symbol": result.get("symbol"),
            "target_symbol": result.get("target_symbol") or result.get("symbol"),
            "status": result.get("status", "unknown"),
            "reason": result.get("reason", "momentum_execution_completed"),
            "order_ids": result.get("order_ids") or [],
            "fill_ids": result.get("fill_ids") or [],
        }
        for key in ("mark_price", "target_price"):
            if key in result:
                decision[key] = result[key]
        return decision

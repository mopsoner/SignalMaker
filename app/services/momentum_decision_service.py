from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.fill import Fill
from app.models.order import Order
from app.models.trade_candidate import TradeCandidate
from app.services.executor_service import ExecutorService


DISPLAY_CONTRACT_FIELDS = (
    "decision_action",
    "symbol",
    "target_symbol",
    "status",
    "reason",
    "order_ids",
    "fill_ids",
)


class MomentumDecisionService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def _latest_open_momentum_candidate(self) -> TradeCandidate | None:
        return self.db.execute(
            select(TradeCandidate)
            .where(TradeCandidate.stage == "momentum", TradeCandidate.status == "open")
            .order_by(TradeCandidate.score.desc(), TradeCandidate.created_at.asc())
            .limit(1)
        ).scalar_one_or_none()

    def _ids_for_symbol(self, model, attr: str, symbol: str) -> list[str]:
        rows = self.db.execute(
            select(getattr(model, attr))
            .where(model.symbol == symbol)
            .order_by(model.created_at.desc() if hasattr(model, "created_at") else getattr(model, attr).desc())
            .limit(20)
        ).scalars().all()
        return [str(row) for row in rows]

    def decision(self) -> dict:
        candidate = self._latest_open_momentum_candidate()
        if candidate is None:
            return {
                "decision_action": "WAIT",
                "symbol": None,
                "target_symbol": None,
                "status": "idle",
                "reason": "no_open_momentum_candidate",
                "order_ids": [],
                "fill_ids": [],
            }

        return {
            "decision_action": "BUY" if candidate.side == "long" else "SELL",
            "symbol": candidate.symbol,
            "target_symbol": candidate.symbol,
            "status": candidate.status,
            "reason": "open_momentum_candidate_ready",
            "order_ids": self._ids_for_symbol(Order, "order_id", candidate.symbol),
            "fill_ids": self._ids_for_symbol(Fill, "fill_id", candidate.symbol),
            "candidate_id": candidate.candidate_id,
        }

    def run_once(self, *, quantity: float = 1.0, mode: str = "paper") -> dict:
        before = self.decision()
        result = ExecutorService(self.db).execute_open_candidates(limit=1, quantity=quantity, mode=mode, sync_momentum_first=False)
        after = self.decision()
        executed = result.get("executed") or []
        if executed:
            candidate_id = executed[0].get("candidate_id")
            candidate = self.db.get(TradeCandidate, candidate_id) if candidate_id else None
            symbol = candidate.symbol if candidate is not None else before.get("symbol")
            order_ids = [str(value) for row in executed for key, value in row.items() if key.endswith("order_id") and value is not None]
            fill_ids = [str(row["fill_id"]) for row in executed if row.get("fill_id") is not None]
            decision = {
                "decision_action": before.get("decision_action") or "BUY",
                "symbol": symbol,
                "target_symbol": symbol,
                "status": "executed",
                "reason": "momentum_execution_completed",
                "order_ids": order_ids,
                "fill_ids": fill_ids,
            }
        else:
            decision = {**after, "status": "skipped" if result.get("skipped") else after.get("status"), "reason": (result.get("skipped") or [{}])[0].get("reason", after.get("reason"))}
        return {"decision": decision, "execution": result}

from __future__ import annotations

from sqlalchemy.orm import Session

from app.core.config import settings

from .kraken_execution_service import ExecutionDisabledError, KrakenExecutionService


class MomentumExecutionService:
    def __init__(self, db: Session, *, execution_service: KrakenExecutionService | None = None) -> None:
        self.db = db
        self._execution_service = execution_service

    @property
    def execution(self) -> KrakenExecutionService:
        if self._execution_service is None:
            self._execution_service = KrakenExecutionService(self.db)
        return self._execution_service

    def execute_decision(self, decision: dict) -> dict:
        left = str(decision.get("action") or "").upper()
        right = str(decision.get("decision_action") or "").upper()
        if left and right and left != right:
            raise ValueError("conflicting action and decision_action")
        action = left or right
        if action not in {"WAIT", "HOLD", "BUY", "SELL", "ROTATE"}:
            raise ValueError(f"unsupported momentum action: {action}")
        if action in {"WAIT", "HOLD"}:
            return {"status": "skipped", "action": action, "orders": []}
        if not settings.momentum_execution_enabled:
            raise ExecutionDisabledError("Momentum execution is disabled")
        if decision.get("should_trade") is False:
            return {"status": "skipped", "reason": "should_trade_false", "orders": []}
        if decision.get("due_now") is False:
            return {"status": "skipped", "reason": "not_due", "orders": []}
        mode = settings.momentum_execution_mode.lower()
        if mode not in {"spot", "margin"}:
            raise ValueError("MOMENTUM_EXECUTION_MODE must be spot or margin")
        if action == "BUY":
            symbol = decision.get("buy_symbol") or decision.get("symbol")
            return {"status": "executed", "action": action, "orders": [self.execution.buy_market(self._symbol(symbol), mode=mode)]}
        if action == "SELL":
            symbol = decision.get("sell_symbol") or decision.get("symbol")
            return {"status": "executed", "action": action, "orders": [self.execution.sell_market(self._symbol(symbol), mode=mode, intent="close_long")]}
        sell = self.execution.sell_market(self._symbol(decision.get("sell_symbol")), mode=mode, intent="close_long")
        if sell.get("status") not in {"filled", "simulated"}:
            return {"status": "pending", "action": action, "orders": [sell]}
        buy = self.execution.buy_market(self._symbol(decision.get("buy_symbol")), mode=mode)
        return {"status": "executed", "action": action, "orders": [sell, buy]}

    @staticmethod
    def _symbol(value: object) -> str:
        symbol = str(value or "").strip().upper()
        if not symbol:
            raise ValueError("decision symbol is required")
        return symbol

from __future__ import annotations

import hashlib
import json

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.app_setting import AppSetting

from .kraken_execution_service import ExecutionDisabledError, KrakenExecutionService


class MomentumLiveExecutionService:
    JOURNAL_CATEGORY = "momentum_execution"
    JOURNAL_KEY = "last_live_decision"

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
        if not settings.momentum_live_enabled:
            raise ExecutionDisabledError("Momentum execution is disabled")
        if decision.get("should_trade") is False:
            return {"status": "skipped", "reason": "should_trade_false", "orders": []}
        if decision.get("due_now") is False:
            return {"status": "skipped", "reason": "not_due", "orders": []}
        decision_id = self._decision_id(decision)
        if not getattr(settings, "kraken_dry_run", True):
            previous = self._execution_receipt()
            if previous and previous.get("decision_id") == decision_id:
                return {
                    "status": "skipped",
                    "reason": "decision_already_submitted",
                    "decision_id": decision_id,
                    "orders": previous.get("orders", []),
                }
        mode = settings.momentum_live_mode.lower()
        if mode not in {"spot", "margin"}:
            raise ValueError("MOMENTUM_LIVE_MODE must be spot or margin")
        if action == "BUY":
            symbol = decision.get("buy_symbol") or decision.get("symbol")
            order = self.execution.buy_market(
                self._symbol(symbol),
                total_notional=settings.kraken_default_total_notional,
                mode=mode,
            )
            result = {"status": "executed", "action": action, "orders": [order]}
            return self._record_execution(decision_id, result)
        if action == "SELL":
            symbol = decision.get("sell_symbol") or decision.get("symbol")
            result = {"status": "executed", "action": action, "orders": [self.execution.sell_market(self._symbol(symbol), mode=mode, intent="close_long")]}
            return self._record_execution(decision_id, result)
        sell = self.execution.sell_market(self._symbol(decision.get("sell_symbol")), mode=mode, intent="close_long")
        if sell.get("status") not in {"filled", "simulated"}:
            return self._record_execution(decision_id, {"status": "pending", "action": action, "orders": [sell]})
        buy = self.execution.buy_market(
            self._symbol(decision.get("buy_symbol")),
            total_notional=settings.kraken_default_total_notional,
            mode=mode,
        )
        return self._record_execution(decision_id, {"status": "executed", "action": action, "orders": [sell, buy]})

    @staticmethod
    def _decision_id(decision: dict) -> str:
        explicit = str(decision.get("decision_id") or "").strip()
        if explicit:
            return explicit
        canonical = json.dumps(decision, sort_keys=True, default=str, separators=(",", ":"))
        return f"legacy-{hashlib.sha256(canonical.encode()).hexdigest()}"

    def _execution_receipt(self) -> dict | None:
        row = self.db.scalar(select(AppSetting).where(
            AppSetting.category == self.JOURNAL_CATEGORY,
            AppSetting.key == self.JOURNAL_KEY,
        ))
        return row.value if row and isinstance(row.value, dict) else None

    def _record_execution(self, decision_id: str, result: dict) -> dict:
        result["decision_id"] = decision_id
        if getattr(settings, "kraken_dry_run", True):
            return result
        row = self.db.scalar(select(AppSetting).where(
            AppSetting.category == self.JOURNAL_CATEGORY,
            AppSetting.key == self.JOURNAL_KEY,
        ))
        receipt = {"decision_id": decision_id, "status": result.get("status"), "orders": result.get("orders", [])}
        if row is None:
            row = AppSetting(category=self.JOURNAL_CATEGORY, key=self.JOURNAL_KEY, value=receipt)
            self.db.add(row)
        else:
            row.value = receipt
        self.db.commit()
        return result

    @staticmethod
    def _symbol(value: object) -> str:
        symbol = str(value or "").strip().upper()
        if not symbol:
            raise ValueError("decision symbol is required")
        return symbol

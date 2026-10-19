from __future__ import annotations

import hashlib
import json
import math

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
        if str(decision.get('action') or '').upper() == 'BATCH':
            return self._execute_portfolio(decision)
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
                total_notional=self._buy_notional(decision, self._symbol(symbol)),
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

    def _buy_notional(self, decision, symbol):
        if '_allocated_notional' in decision:
            amount = float(decision['_allocated_notional'])
            if not math.isfinite(amount) or amount <= 0:
                raise ValueError('Invalid allocated notional')
            return min(amount, getattr(settings, 'live_max_notional_per_trade', amount))
        divisor = decision.get('allocation_divisor')
        if divisor is None:
            return settings.kraken_default_total_notional
        if isinstance(divisor, bool) or not isinstance(divisor, int) or not 1 <= divisor <= getattr(settings, 'momentum_max_positions', 3):
            raise ValueError('allocation_divisor exceeds configured position limit')
        if getattr(settings, 'kraken_dry_run', True):
            # This dry-run budget represents the remaining available quote cash.
            free = float(decision.get('total_notional', settings.kraken_default_total_notional)) * divisor
        else:
            free = self.execution.client.free_balance(self.execution.rules.quote_asset(symbol))
            free = max(0.0, (free - settings.kraken_quote_reserve) * settings.kraken_buy_balance_ratio)
        amount = free / divisor
        cap = getattr(settings, 'live_max_notional_per_trade', amount)
        if not math.isfinite(amount) or amount <= 0:
            raise ValueError('No usable cash for portfolio entry')
        return min(amount, cap)

    def _execute_portfolio(self, decision):
        if str(decision.get('decision_action') or 'BATCH').upper() != 'BATCH':
            raise ValueError('conflicting portfolio actions')
        if not settings.momentum_live_enabled:
            raise ExecutionDisabledError('Momentum execution is disabled')
        if not decision.get('should_trade') or not decision.get('due_now'):
            return {'status': 'skipped', 'reason': 'portfolio_not_due', 'orders': []}
        operations = decision.get('operations')
        if not isinstance(operations, list) or len(operations) > 40:
            raise ValueError('Invalid portfolio operations')
        # Validate the entire plan before any order, and require exits before buys.
        bought = set()
        selling = set()
        buying_started = False
        for operation in operations:
            action = str(operation.get('action', '')).upper()
            symbol = self._symbol(operation.get('symbol'))
            if action == 'SELL':
                if buying_started or symbol in selling:
                    raise ValueError('Portfolio sells must be unique and precede buys')
                selling.add(symbol)
            elif action == 'BUY':
                buying_started = True
                divisor = operation.get('allocation_divisor')
                if symbol in bought or symbol in selling or isinstance(divisor, bool) or not isinstance(divisor, int) or not 1 <= divisor <= getattr(settings, 'momentum_max_positions', 3):
                    raise ValueError('Invalid portfolio buy')
                bought.add(symbol)
            else:
                raise ValueError('Only BUY and SELL portfolio operations are supported')
        if len(bought) > getattr(settings, 'momentum_max_positions', 3) or len(selling) > 20:
            raise ValueError('Portfolio operations exceed configured position limit')
        parent_id = self._decision_id(decision)
        orders = []
        for index, operation in enumerate(operations):
            child = MomentumLiveExecutionService(self.db, execution_service=self.execution)
            # Keep a receipt per operation so retries never repeat completed legs.
            child.JOURNAL_KEY = f'portfolio:{parent_id}:{index}'
            operation = dict(operation)
            if operation['action'].upper() == 'BUY':
                operation['_allocated_notional'] = self._portfolio_allocation(parent_id, operation)
            result = child.execute_decision({**operation, 'decision_id': f'{parent_id}:{index}',
                'decision_action': operation['action'], 'should_trade': True, 'due_now': True})
            orders.extend(result.get('orders', []))
            if any(order.get('status') not in {'filled', 'simulated'} for order in result.get('orders', [])):
                return {'status': 'pending', 'decision_id': parent_id, 'orders': orders}
        return {'status': 'executed', 'decision_id': parent_id, 'orders': orders}

    def _portfolio_allocation(self, parent_id, operation):
        if getattr(settings, 'kraken_dry_run', True):
            return self._buy_notional(operation, self._symbol(operation['symbol']))
        quote = self.execution.rules.quote_asset(self._symbol(operation['symbol']))
        key = f'portfolio-budget:{parent_id}:{quote}'
        row = self.db.scalar(select(AppSetting).where(AppSetting.category == self.JOURNAL_CATEGORY,
                                                     AppSetting.key == key))
        if row:
            return float(row.value['notional'])
        # Freeze one allocation per quote currency for the entire batch, including retries.
        amount = self._buy_notional(operation, self._symbol(operation['symbol']))
        self.db.add(AppSetting(category=self.JOURNAL_CATEGORY, key=key, value={'notional': amount}))
        self.db.commit()
        return amount

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

from __future__ import annotations

from uuid import uuid4

from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.order import Order

from .kraken_client import KrakenClient
from .kraken_margin_client import KrakenMarginClient
from .kraken_symbol_rules import KrakenSymbolRules


class ExecutionDisabledError(RuntimeError):
    pass


class ExecutionConfigurationError(RuntimeError):
    pass


class CorrectableExecutionError(ValueError):
    """A pre-submission rejection which can become valid on a later attempt."""


class InsufficientNotionalError(CorrectableExecutionError):
    pass


class InsufficientBalanceError(CorrectableExecutionError):
    pass


class KrakenExecutionService:
    def __init__(self, db: Session, *, client: KrakenClient | None = None, rules: KrakenSymbolRules | None = None) -> None:
        self.db = db
        self.client = client or KrakenClient(settings.kraken_base_url, settings.kraken_api_key, settings.kraken_secret_key, settings.kraken_dry_run)
        self.rules = rules or KrakenSymbolRules(self.client, [x.strip() for x in settings.kraken_quote_assets.split(",") if x.strip()])
        self.margin = KrakenMarginClient(self.client, self.rules)

    def _guard(self, mode: str = "spot") -> None:
        if not settings.kraken_execution_enabled:
            raise ExecutionDisabledError("Kraken execution is disabled")
        if mode not in {"spot", "margin"}:
            raise ExecutionConfigurationError("execution mode must be spot or margin")
        if mode == "margin" and not settings.kraken_margin_execution_enabled:
            raise ExecutionDisabledError("Kraken margin execution is disabled")
        if not settings.kraken_dry_run and not self.client.is_configured():
            raise ExecutionConfigurationError("Kraken credentials are required for real execution")

    def _record(self, result: dict, symbol: str, side: str, quantity: float, mode: str, order_type: str = "market") -> dict:
        order_id = str(result.get("order_id") or uuid4())
        if self.db.get(Order, order_id) is None:
            self.db.add(Order(order_id=order_id, symbol=symbol.upper(), side=side, order_type=order_type, status=str(result.get("status", "pending")), quantity=quantity, meta={"exchange": "kraken", "execution_mode": mode, "dry_run": bool(result.get("dry_run")), "response": result}))
            self.db.commit()
        return result

    def _margin_leverage(self, symbol: str, side: str, requested: int | None) -> tuple[int, tuple[int, ...]]:
        configured_max = settings.kraken_margin_max_leverage
        supported = self.rules.supported_leverages(symbol, side)
        if requested is None:
            return self.rules.max_supported_leverage(symbol, side, configured_max), supported
        effective = int(requested)
        if effective > configured_max:
            raise ValueError("requested leverage exceeds configured maximum")
        return self.rules.validate_leverage(symbol, side, effective), supported

    def buy_market(self, symbol: str, total_notional: float | None = None, *, mode: str = "spot", leverage: int | None = None) -> dict:
        self._guard(mode)
        desired_total = float(
            settings.kraken_default_total_notional
            if total_notional is None
            else total_notional
        )
        minimum_total = float(settings.live_min_total_notional_per_trade)
        if desired_total < minimum_total:
            raise InsufficientNotionalError(f"requested total notional {desired_total:.2f} is below required minimum {minimum_total:.2f}")
        price = self.client.current_price(symbol)
        effective_leverage = 1
        supported_leverages: tuple[int, ...] = ()
        if mode == "margin":
            effective_leverage, supported_leverages = self._margin_leverage(symbol, "buy", leverage)
        quantity = self.rules.quantity_for_total_notional(symbol, desired_total, price, minimum_total)
        normalized_total = float(quantity) * price
        normalized_own_quote = normalized_total / effective_leverage
        usable_balance = normalized_own_quote
        if not settings.kraken_dry_run:
            free = self.client.free_balance(self.rules.quote_asset(symbol))
            usable_balance = max(0.0, (free - settings.kraken_quote_reserve) * settings.kraken_buy_balance_ratio)
            possible_total = usable_balance * effective_leverage
            if usable_balance < normalized_own_quote:
                raise InsufficientBalanceError(
                    f"insufficient balance for required minimum total notional {minimum_total:.2f}; "
                    f"possible total notional {possible_total:.2f}, effective leverage {effective_leverage}, "
                    f"usable quote balance {usable_balance:.2f}"
                )
        result = self.client.place_market_entry(symbol, "buy", quantity) if mode == "spot" else self.margin.margin_order(symbol, "buy", quantity, effective_leverage)
        result.update({"mode": mode, "own_quote_amount": normalized_own_quote, "borrowed_notional": normalized_total - normalized_own_quote, "total_notional": normalized_total, "price": price})
        if mode == "margin":
            result.update({"configured_max_leverage": settings.kraken_margin_max_leverage, "supported_leverages": list(supported_leverages), "effective_leverage": effective_leverage})
        return self._record(result, symbol, "buy", float(quantity), mode)

    def sell_market(self, symbol: str, quantity: float | None = None, *, mode: str = "spot", leverage: int | None = None, intent: str = "close_long") -> dict:
        self._guard(mode)
        if mode == "margin" and intent == "open_short" and not settings.kraken_margin_shorts_enabled:
            raise ExecutionDisabledError("Kraken margin shorts are disabled")
        if mode == "margin" and intent not in {"close_long", "reduce_long", "open_short"}:
            raise ValueError("invalid margin sell intent")
        raw_quantity = quantity
        if raw_quantity is None:
            if mode == "margin":
                raise ValueError("quantity is required for margin sells")
            raw_quantity = self.client.free_balance(self.rules.base_asset(symbol))
        normalized = self.rules.normalize_market_quantity(symbol, raw_quantity)
        lev = 1
        supported_leverages: tuple[int, ...] = ()
        if mode == "margin":
            lev, supported_leverages = self._margin_leverage(symbol, "sell", leverage)
        result = self.client.place_market_entry(symbol, "sell", normalized) if mode == "spot" else self.margin.margin_order(symbol, "sell", normalized, lev)
        result.update({"mode": mode, "intent": intent})
        if mode == "margin":
            result.update({"configured_max_leverage": settings.kraken_margin_max_leverage, "supported_leverages": list(supported_leverages), "effective_leverage": lev})
        return self._record(result, symbol, "sell", float(normalized), mode)

    def cancel_order(self, symbol: str, order_id: str, *, mode: str = "spot") -> dict:
        self._guard(mode)
        return self.client.cancel_order(symbol, order_id)

    def get_order(self, symbol: str, order_id: str, *, mode: str = "spot") -> dict:
        self._guard(mode)
        return self.client.get_order(symbol, order_id)

    def place_take_profit(self, symbol: str, side: str, quantity: float, price: float, *, mode: str = "spot", leverage: int | None = None) -> dict:
        self._guard(mode)
        normalized = self.rules.normalize_market_quantity(symbol, quantity)
        if mode == "margin":
            lev, _ = self._margin_leverage(symbol, side, leverage)
            result = self.margin.close_limit(symbol, side, normalized, price, lev)
        else:
            result = self.client.place_exit_limit(symbol, side, normalized, price)
        return self._record(result, symbol, side, float(normalized), mode, "limit")

    def account_summary(self) -> dict:
        self._guard()
        if settings.kraken_dry_run:
            return {"exchange": "kraken", "dry_run": True, "balances": {}, "margin_positions": {}}
        return {"exchange": "kraken", "dry_run": False, "balances": self.client.balance(), "margin_positions": self.client.open_margin_positions()}

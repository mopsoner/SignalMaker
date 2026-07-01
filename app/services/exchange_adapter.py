from __future__ import annotations

import math
from typing import Any, Protocol

from app.services.runtime_settings import _coerce_bool, load_runtime_settings
from raspberry_executor.kraken_client import KrakenClient


class ExecutionAdapter(Protocol):
    exchange_name: str

    def is_configured(self) -> bool: ...

    def current_price(self, symbol: str) -> float: ...

    def normalize_order(self, symbol: str, quantity: float, target_price: float | None, stop_price: float | None) -> dict[str, Any]: ...

    def average_fill_price(self, order_payload: dict[str, Any], fallback: float | None = None) -> float | None: ...

    def place_market_entry(self, symbol: str, side: str, quantity: float | str) -> dict[str, Any]: ...

    def place_exit_limit(self, symbol: str, side: str, quantity: float | str, price: float | str) -> dict[str, Any]: ...

    def place_stop_loss(self, symbol: str, side: str, quantity: float | str, stop_price: float | str) -> dict[str, Any]: ...

    def get_order(self, symbol: str, order_id: str | int) -> dict[str, Any]: ...


class KrakenExchangeAdapter:
    exchange_name = "kraken"

    def __init__(self, db=None) -> None:
        runtime = load_runtime_settings(db)
        kraken = runtime.get("kraken", {}) if isinstance(runtime.get("kraken"), dict) else {}
        live = runtime.get("live", {}) if isinstance(runtime.get("live"), dict) else {}
        self.client = KrakenClient(
            str(kraken.get("kraken_base_url") or ""),
            str(kraken.get("kraken_api_key") or ""),
            str(kraken.get("kraken_secret_key") or ""),
            dry_run=not _coerce_bool(live.get("live_trading_enabled"), default=False),
        )

    def is_configured(self) -> bool:
        return self.client.is_configured()

    def current_price(self, symbol: str) -> float:
        return self.client.current_price(symbol)

    def normalize_order(self, symbol: str, quantity: float, target_price: float | None, stop_price: float | None) -> dict[str, Any]:
        mark = self.current_price(symbol)
        normalized_qty = float(quantity)
        if not math.isfinite(normalized_qty) or normalized_qty <= 0:
            raise RuntimeError(f"Invalid quantity {quantity} for {symbol}")
        out: dict[str, Any] = {"quantity": normalized_qty, "mark_price": mark}
        if target_price is not None:
            out["target_price"] = float(target_price)
        if stop_price is not None:
            out["stop_price"] = float(stop_price)
        return out

    def average_fill_price(self, order_payload: dict[str, Any], fallback: float | None = None) -> float | None:
        return self.client.average_fill_price(order_payload, fallback=fallback)

    def place_market_entry(self, symbol: str, side: str, quantity: float | str) -> dict[str, Any]:
        return self.client.place_market_entry(symbol, side, quantity)

    def place_exit_limit(self, symbol: str, side: str, quantity: float | str, price: float | str) -> dict[str, Any]:
        return self.client.place_exit_limit(symbol, side, quantity, price)

    def place_stop_loss(self, symbol: str, side: str, quantity: float | str, stop_price: float | str) -> dict[str, Any]:
        return self.client.place_stop_loss(symbol, side, quantity, stop_price)

    def get_order(self, symbol: str, order_id: str | int) -> dict[str, Any]:
        return self.client.get_order(symbol, order_id)


def configured_exchange_name(db=None) -> str:
    runtime = load_runtime_settings(db)
    executor = runtime.get("executor", {}) if isinstance(runtime.get("executor"), dict) else {}
    return str(executor.get("execution_exchange") or "kraken").strip().lower()


def create_execution_adapter(db=None) -> ExecutionAdapter:
    name = configured_exchange_name(db)
    if name in {"kraken", "kraken_pro"}:
        return KrakenExchangeAdapter(db)
    raise RuntimeError(f"unsupported_execution_exchange:{name}")

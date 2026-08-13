from __future__ import annotations

from typing import Any

from .kraken_client import KrakenClient
from .kraken_symbol_rules import KrakenSymbolRules


class KrakenMarginClient:
    """Kraken cross Spot Margin adapter (borrowing is implicit in AddOrder)."""

    def __init__(self, client: KrakenClient, rules: KrakenSymbolRules) -> None:
        self.client = client
        self.rules = rules

    def margin_order(self, symbol: str, side: str, quantity: float | str, leverage: int) -> dict[str, Any]:
        self.rules.validate_leverage(symbol, side, leverage)
        result = self.client.place_market_entry(symbol, side, quantity, leverage=leverage)
        return {**result, "mode": "margin", "margin_account_mode": "cross"}

    def get_margin_order(self, symbol: str, order_id: str) -> dict[str, Any]:
        return self.client.get_order(symbol, order_id)

    def open_positions(self) -> dict[str, Any]:
        return self.client.open_margin_positions()

    def open_margin_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        return self.client.open_orders(symbol)

    def cancel_margin_order(self, symbol: str, order_id: str) -> dict[str, Any]:
        return self.client.cancel_order(symbol, order_id)

    def margin_oco_sell(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("Kraken Spot Margin OCO is not supported")

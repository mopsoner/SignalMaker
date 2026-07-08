from __future__ import annotations

import time
from typing import Any

from raspberry_executor.kraken_client import KrakenClient
from raspberry_executor.margin_settings import margin_multiplier


def _kraken_order_type(order_type: str) -> str:
    value = str(order_type or "MARKET").upper()
    if value == "MARKET":
        return "market"
    if value == "LIMIT":
        return "limit"
    if value in {"STOP_LOSS", "STOP-LOSS"}:
        return "stop-loss"
    if value in {"STOP_LOSS_LIMIT", "STOP-LOSS-LIMIT"}:
        return "stop-loss-limit"
    return value.lower().replace("_", "-")


class KrakenMarginClient:
    """Kraken spot-margin adapter for the existing MarginClient surface.

    Kraken Spot margin does not expose Kraken-style isolated accounts or an
    explicit borrow/repay endpoint. Borrowing is implicit when an order is sent
    with a `leverage` value; repayment happens when the margin position is
    closed or settled. This adapter keeps the executor contract intact and
    records those operations as implicit borrow/repay payloads.
    """

    def __init__(self, kraken: KrakenClient, *, isolated: bool = False, dry_run: bool = True, leverage: float | str | None = None) -> None:
        self.kraken = kraken  # Compatibility with existing manager attribute names.
        self.kraken = kraken
        self.isolated = False
        self.requested_isolated = isolated
        self.dry_run = dry_run or kraken.dry_run
        self.leverage_override = leverage

    def is_isolated_value(self) -> str:
        return "FALSE"

    @staticmethod
    def _format_leverage(leverage: float | str) -> str:
        value = max(2.0, min(5.0, float(leverage)))
        if value.is_integer():
            return str(int(value))
        return f"{value:.2f}".rstrip("0").rstrip(".")

    def leverage(self) -> str:
        configured = self.leverage_override if self.leverage_override is not None else margin_multiplier()
        return self._format_leverage(configured)

    def ensure_isolated_account(self, symbol: str) -> dict:
        if self.requested_isolated:
            return {"status": "cross_margin_required", "exchange": "kraken", "symbol": symbol.upper(), "message": "Kraken Spot margin is cross-margin; isolated margin is not available."}
        return {"status": "cross_margin", "exchange": "kraken", "symbol": symbol.upper()}

    def isolated_account(self, symbol: str) -> dict:
        if self.dry_run:
            return {"assets": [], "exchange": "kraken", "mode": "cross_margin"}
        return {"balances": self.kraken.account(), "exchange": "kraken", "mode": "cross_margin", "symbol": symbol.upper()}

    def margin_free_balance(self, symbol: str, asset: str) -> float:
        return self.kraken.free_balance(asset)

    def max_borrowable(self, symbol: str, asset: str) -> float:
        if self.dry_run:
            return 0.0
        # Kraken validates borrow capacity at order submission. Returning 0 lets
        # MarginOrderManager keep the requested borrow amount and fail safely if
        # Kraken rejects the leveraged order.
        return 0.0

    def borrow(self, symbol: str, asset: str, amount: str) -> dict:
        return {"status": "implicit_borrow_on_order", "exchange": "kraken", "symbol": symbol.upper(), "asset": asset.upper(), "amount": amount, "leverage": self.leverage()}

    def repay(self, symbol: str, asset: str, amount: str) -> dict:
        return {"status": "implicit_repay_on_close_or_settle", "exchange": "kraken", "symbol": symbol.upper(), "asset": asset.upper(), "amount": amount}

    def transfer_spot_to_margin(self, symbol: str, asset: str, amount: str) -> dict:
        return {"status": "not_required", "exchange": "kraken", "action": "spot_to_margin", "symbol": symbol.upper(), "asset": asset.upper(), "amount": amount, "message": "Kraken Spot margin uses the same spot wallet collateral."}

    def margin_order(self, symbol: str, side: str, quantity: str, order_type: str = "MARKET", *, price: str | None = None, time_in_force: str | None = None, leverage: float | str | None = None) -> dict:
        side_lower = side.lower()
        type_lower = _kraken_order_type(order_type)
        effective_leverage = self._format_leverage(leverage) if leverage is not None else self.leverage()
        if self.dry_run:
            order_prefix = "dry-kraken-margin-entry" if type_lower == "market" else "dry-kraken-margin-tp"
            status = "FILLED" if type_lower == "market" else "NEW"
            return {"orderId": f"{order_prefix}-{int(time.time())}", "status": status, "executedQty": str(quantity) if type_lower == "market" else "0", "dry_run": True, "exchange": "kraken", "symbol": symbol.upper(), "side": side.upper(), "type": order_type, "quantity": quantity, "price": price, "leverage": effective_leverage}
        params: dict[str, Any] = {"pair": self.kraken._pair_key(symbol), "type": side_lower, "ordertype": type_lower, "volume": quantity, "leverage": effective_leverage}
        if price is not None:
            params["price"] = price
        if time_in_force:
            params["timeinforce"] = time_in_force.upper()
        if type_lower != "market":
            params["reduce_only"] = True
        result = self.kraken._signed("POST", "/0/private/AddOrder", params)
        txid = (result.get("txid") or [None])[0]
        return {"orderId": txid, "txid": result.get("txid"), "status": "NEW", "exchange": "kraken", "symbol": symbol.upper(), "side": side.upper(), "type": order_type, "quantity": quantity, "price": price, "leverage": effective_leverage, "result": result}

    def get_margin_order(self, symbol: str, order_id: str | int) -> dict:
        return self.kraken.get_order(symbol, order_id)

    def open_margin_orders(self, symbol: str) -> list[dict]:
        return self.kraken.open_orders(symbol)

    def margin_oco_sell(self, symbol: str, quantity: str, target_price: str, stop_price: str, stop_limit_price: str) -> dict:
        raise RuntimeError("kraken_margin_oco_not_supported: use take_profit_only")

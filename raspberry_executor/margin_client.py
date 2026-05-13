import time
from typing import Any

from raspberry_executor.binance_client import BinanceClient


class MarginClient:
    def __init__(self, binance: BinanceClient, *, isolated: bool = True, dry_run: bool = True) -> None:
        self.binance = binance
        self.isolated = isolated
        self.dry_run = dry_run or binance.dry_run

    def is_isolated_value(self) -> str:
        return "TRUE" if self.isolated else "FALSE"

    def ensure_isolated_account(self, symbol: str) -> dict:
        if not self.isolated:
            return {"status": "cross_margin"}
        if self.dry_run:
            return {"status": "dry_run", "action": "enable_isolated_account", "symbol": symbol.upper()}
        try:
            return self.binance._signed("POST", "/sapi/v1/margin/isolated/account", {"symbol": symbol.upper()})
        except Exception as exc:
            text = str(exc)
            if "already" in text.lower() or "exists" in text.lower() or "-11000" in text:
                return {"status": "already_enabled", "symbol": symbol.upper(), "message": text}
            raise

    def isolated_account(self, symbol: str) -> dict:
        if self.dry_run:
            return {"assets": []}
        if self.isolated:
            return self.binance._signed("GET", "/sapi/v1/margin/isolated/account", {"symbols": symbol.upper()})
        return self.binance._signed("GET", "/sapi/v1/margin/account", {})

    def margin_free_balance(self, symbol: str, asset: str) -> float:
        asset = asset.upper()
        if self.dry_run:
            return 0.0
        data = self.isolated_account(symbol)
        if self.isolated:
            rows = data.get("assets") or []
            for row in rows:
                for key in ("baseAsset", "quoteAsset"):
                    item = row.get(key) or {}
                    if str(item.get("asset") or "").upper() == asset:
                        return float(item.get("free") or 0)
            return 0.0
        for item in data.get("userAssets") or []:
            if str(item.get("asset") or "").upper() == asset:
                return float(item.get("free") or 0)
        return 0.0

    def max_borrowable(self, symbol: str, asset: str) -> float:
        if self.dry_run:
            return 0.0
        params: dict[str, Any] = {"asset": asset.upper()}
        if self.isolated:
            params["isolatedSymbol"] = symbol.upper()
        data = self.binance._signed("GET", "/sapi/v1/margin/maxBorrowable", params)
        return float(data.get("amount") or data.get("borrowLimit") or 0)

    def borrow(self, symbol: str, asset: str, amount: str) -> dict:
        payload = {"asset": asset.upper(), "amount": amount, "type": "BORROW", "isIsolated": self.is_isolated_value()}
        if self.isolated:
            payload["symbol"] = symbol.upper()
        if self.dry_run:
            return {"status": "dry_run", "action": "borrow", **payload}
        return self.binance._signed("POST", "/sapi/v1/margin/borrow-repay", payload)

    def transfer_spot_to_margin(self, symbol: str, asset: str, amount: str) -> dict:
        if self.dry_run:
            return {"status": "dry_run", "action": "spot_to_margin", "symbol": symbol.upper(), "asset": asset.upper(), "amount": amount}
        if self.isolated:
            return self.binance._signed("POST", "/sapi/v1/margin/isolated/transfer", {"asset": asset.upper(), "symbol": symbol.upper(), "amount": amount, "transFrom": "SPOT", "transTo": "ISOLATED_MARGIN"})
        return self.binance._signed("POST", "/sapi/v1/margin/transfer", {"asset": asset.upper(), "amount": amount, "type": 1})

    def margin_order(self, symbol: str, side: str, quantity: str, order_type: str = "MARKET") -> dict:
        payload = {"symbol": symbol.upper(), "side": side.upper(), "type": order_type, "quantity": quantity, "isIsolated": self.is_isolated_value(), "newOrderRespType": "FULL"}
        if self.dry_run:
            return {
                "orderId": f"dry-margin-entry-{int(time.time())}",
                "status": "FILLED",
                "executedQty": str(quantity),
                "dry_run": True,
                **payload,
            }
        return self.binance._signed("POST", "/sapi/v1/margin/order", payload)

    def get_margin_order(self, symbol: str, order_id: str | int) -> dict:
        if str(order_id).startswith("dry-") or str(order_id).startswith("dry_"):
            return {"orderId": order_id, "status": "NEW", "dry_run": True}
        payload: dict[str, Any] = {"symbol": symbol.upper(), "orderId": int(order_id), "isIsolated": self.is_isolated_value()}
        return self.binance._signed("GET", "/sapi/v1/margin/order", payload)

    def open_margin_orders(self, symbol: str) -> list[dict]:
        if self.dry_run:
            return []
        payload: dict[str, Any] = {"symbol": symbol.upper(), "isIsolated": self.is_isolated_value()}
        data = self.binance._signed("GET", "/sapi/v1/margin/openOrders", payload)
        return data if isinstance(data, list) else []

    def margin_oco_sell(self, symbol: str, quantity: str, target_price: str, stop_price: str, stop_limit_price: str) -> dict:
        payload = {"symbol": symbol.upper(), "side": "SELL", "quantity": quantity, "price": target_price, "stopPrice": stop_price, "stopLimitPrice": stop_limit_price, "stopLimitTimeInForce": "GTC", "isIsolated": self.is_isolated_value(), "newOrderRespType": "FULL"}
        if self.dry_run:
            return {"orderListId": "dry-margin-oco", "dry_run": True, "orders": [{"orderId": "dry-margin-tp"}, {"orderId": "dry-margin-sl"}], **payload}
        return self.binance._signed("POST", "/sapi/v1/margin/order/oco", payload)

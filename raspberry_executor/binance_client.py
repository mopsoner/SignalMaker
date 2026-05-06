import hashlib
import hmac
import time
from typing import Any
from urllib.parse import urlencode

import requests


class BinanceClient:
    def __init__(self, base_url: str, api_key: str, secret_key: str, dry_run: bool = True) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.secret_key = secret_key
        self.dry_run = dry_run
        self.session = requests.Session()

    def is_configured(self) -> bool:
        return bool(self.api_key and self.secret_key)

    def _signed(self, method: str, path: str, params: dict[str, Any] | None = None) -> dict:
        if not self.is_configured():
            raise RuntimeError("Binance API credentials are missing")
        payload = dict(params or {})
        payload["timestamp"] = int(time.time() * 1000)
        payload.setdefault("recvWindow", 5000)
        query = urlencode(payload, doseq=True)
        signature = hmac.new(self.secret_key.encode(), query.encode(), hashlib.sha256).hexdigest()
        headers = {"X-MBX-APIKEY": self.api_key}
        response = self.session.request(
            method.upper(),
            f"{self.base_url}{path}?{query}&signature={signature}",
            headers=headers,
            timeout=20,
        )
        if not response.ok:
            raise RuntimeError(f"Binance {method.upper()} {path} failed status={response.status_code} body={response.text}")
        return response.json()

    def current_price(self, symbol: str) -> float:
        response = self.session.get(
            f"{self.base_url}/api/v3/ticker/price",
            params={"symbol": symbol.upper()},
            timeout=10,
        )
        response.raise_for_status()
        return float(response.json()["price"])

    @staticmethod
    def average_fill_price(order_payload: dict[str, Any], fallback: float | None = None) -> float | None:
        fills = order_payload.get("fills") or []
        if fills:
            qty = sum(float(item.get("qty", 0.0)) for item in fills)
            quote = sum(float(item.get("qty", 0.0)) * float(item.get("price", 0.0)) for item in fills)
            if qty > 0:
                return quote / qty
        executed_qty = float(order_payload.get("executedQty", 0.0) or 0.0)
        quote_qty = float(order_payload.get("cummulativeQuoteQty", 0.0) or 0.0)
        if executed_qty > 0 and quote_qty > 0:
            return quote_qty / executed_qty
        return fallback

    def place_market_entry(self, symbol: str, side: str, quantity: float) -> dict:
        side_upper = "BUY" if side.lower() == "long" else "SELL"
        if self.dry_run:
            price = self.current_price(symbol)
            return {
                "orderId": f"dry-entry-{int(time.time())}",
                "status": "FILLED",
                "symbol": symbol.upper(),
                "side": side_upper,
                "executedQty": str(quantity),
                "fills": [{"price": str(price), "qty": str(quantity)}],
                "dry_run": True,
            }
        return self._signed("POST", "/api/v3/order", {
            "symbol": symbol.upper(),
            "side": side_upper,
            "type": "MARKET",
            "quantity": quantity,
            "newOrderRespType": "FULL",
        })

    def place_exit_limit(self, symbol: str, side: str, quantity: float, price: float) -> dict:
        exit_side = "SELL" if side.lower() == "long" else "BUY"
        if self.dry_run:
            return {"orderId": f"dry-tp-{int(time.time())}", "status": "NEW", "price": str(price), "dry_run": True}
        return self._signed("POST", "/api/v3/order", {
            "symbol": symbol.upper(),
            "side": exit_side,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": quantity,
            "price": price,
        })

    def place_stop_loss(self, symbol: str, side: str, quantity: float, stop_price: float) -> dict:
        exit_side = "SELL" if side.lower() == "long" else "BUY"
        if self.dry_run:
            return {"orderId": f"dry-sl-{int(time.time())}", "status": "NEW", "stopPrice": str(stop_price), "dry_run": True}
        return self._signed("POST", "/api/v3/order", {
            "symbol": symbol.upper(),
            "side": exit_side,
            "type": "STOP_LOSS_LIMIT",
            "timeInForce": "GTC",
            "quantity": quantity,
            "price": stop_price,
            "stopPrice": stop_price,
        })

    def get_order(self, symbol: str, order_id: str | int) -> dict:
        if str(order_id).startswith("dry-"):
            return {"orderId": order_id, "status": "NEW", "dry_run": True}
        return self._signed("GET", "/api/v3/order", {"symbol": symbol.upper(), "orderId": int(order_id)})

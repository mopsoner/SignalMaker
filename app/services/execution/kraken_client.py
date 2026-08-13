from __future__ import annotations

import base64
import hashlib
import hmac
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from typing import Any, Protocol
from urllib.parse import urlencode

import requests


KRAKEN_BASE_URL = "https://api.kraken.com"
KRAKEN_PRIVATE_PREFIX = "/0/private/"
KRAKEN_REQUEST_TIMEOUT_SECONDS = 20


class HTTPResponse(Protocol):
    def raise_for_status(self) -> None: ...

    def json(self) -> Any: ...


class HTTPSession(Protocol):
    def get(self, url: str, *, params: Mapping[str, Any], timeout: int) -> HTTPResponse: ...
    def post(
        self,
        url: str,
        *,
        data: str,
        headers: Mapping[str, str],
        timeout: int,
    ) -> HTTPResponse: ...


class KrakenNonceGenerator:
    """Generate monotonically increasing millisecond nonces, safely across threads."""

    def __init__(self, time_ms: Callable[[], int] | None = None) -> None:
        self._time_ms = time_ms or (lambda: time.time_ns() // 1_000_000)
        self._previous_nonce = 0
        self._lock = threading.Lock()

    def __call__(self) -> int:
        with self._lock:
            nonce = max(int(self._time_ms()), self._previous_nonce + 1)
            self._previous_nonce = nonce
            return nonce


class KrakenAPIError(RuntimeError):
    """An error returned in an otherwise successful Kraken API response."""

    def __init__(self, path: str, errors: list[str] | tuple[str, ...]) -> None:
        self.path = path
        self.errors = tuple(str(error) for error in errors)
        super().__init__(f"Kraken POST {path} failed errors={list(self.errors)!r}")


class KrakenClient:
    """Minimal Kraken Spot client for authenticated private REST requests."""

    exchange_name = "kraken"

    def __init__(
        self,
        base_url: str = KRAKEN_BASE_URL,
        api_key: str = "",
        secret_key: str = "",
        dry_run: bool = True,
        *,
        session: HTTPSession | None = None,
        nonce_provider: Callable[[], int | str] | None = None,
    ) -> None:
        self.base_url = (base_url or KRAKEN_BASE_URL).rstrip("/")
        self.api_key = api_key
        self.secret_key = secret_key
        self.dry_run = dry_run
        self.session = session or requests.Session()
        self._nonce_provider = nonce_provider or KrakenNonceGenerator()

    def is_configured(self) -> bool:
        return bool(self.api_key and self.secret_key)

    def recv_window_ms(self) -> int:
        # Kraken private requests use nonces rather than a receive window.
        return 0

    def _signed(
        self,
        method: str,
        path: str,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        if method.upper() != "POST":
            raise ValueError("Kraken private REST endpoints require POST")
        if not path.startswith(KRAKEN_PRIVATE_PREFIX):
            raise ValueError(f"Kraken private REST path must start with {KRAKEN_PRIVATE_PREFIX}")
        if not self.is_configured():
            raise RuntimeError("Kraken API credentials are missing")

        nonce = str(self._nonce_provider())
        payload = dict(params or {})
        payload["nonce"] = nonce
        encoded_body = urlencode(payload)
        message = path.encode() + hashlib.sha256((nonce + encoded_body).encode()).digest()
        signature = hmac.new(
            base64.b64decode(self.secret_key),
            message,
            hashlib.sha512,
        ).digest()
        headers = {
            "API-Key": self.api_key,
            "API-Sign": base64.b64encode(signature).decode(),
            "Content-Type": "application/x-www-form-urlencoded",
        }

        response = self.session.post(
            f"{self.base_url}{path}",
            data=encoded_body,
            headers=headers,
            timeout=KRAKEN_REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()
        errors = data.get("error") or []
        if errors:
            raise KrakenAPIError(path, errors)
        return data.get("result") or {}

    def _public(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        response = self.session.get(f"{self.base_url}{path}", params=dict(params or {}), timeout=KRAKEN_REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        data = response.json()
        errors = data.get("error") or []
        if errors:
            raise KrakenAPIError(path, errors)
        return data.get("result") or {}

    @staticmethod
    def _asset_name(asset: str) -> str:
        value = str(asset or "").upper()
        if value in {"XBT", "XXBT"}:
            return "BTC"
        return value.lstrip("XZ")

    def asset_pairs(self) -> dict[str, Any]:
        return self._public("/0/public/AssetPairs", {"assetVersion": 1})

    def pair_info(self, symbol: str) -> dict[str, Any]:
        wanted = symbol.upper().replace("/", "").replace("BTC", "XBT")
        for key, row in self.asset_pairs().items():
            names = {str(key), str(row.get("altname") or ""), str(row.get("wsname") or "")}
            normalized = {name.upper().replace("/", "").replace("BTC", "XBT") for name in names}
            if wanted in normalized:
                return {**row, "pair_key": key, "baseAsset": self._asset_name(row.get("base", "")), "quoteAsset": self._asset_name(row.get("quote", ""))}
        raise ValueError(f"Kraken pair not found: {symbol}")

    def current_price(self, symbol: str) -> float:
        info = self.pair_info(symbol)
        rows = self._public("/0/public/Ticker", {"pair": info["pair_key"]})
        if not rows:
            raise KrakenAPIError("/0/public/Ticker", ["empty ticker result"])
        return float((next(iter(rows.values())).get("c") or [0])[0])

    def balance(self) -> dict[str, Any]:
        return self._signed("POST", "/0/private/Balance")

    account = balance

    def free_balance(self, asset: str) -> float:
        if self.dry_run:
            return 0.0
        wanted = self._asset_name(asset)
        for name, value in self.balance().items():
            if self._asset_name(name) == wanted:
                return float(value or 0)
        return 0.0

    def place_market_entry(self, symbol: str, side: str, quantity: float | str, *, leverage: int | None = None) -> dict[str, Any]:
        side = side.strip().lower()
        if side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        payload: dict[str, Any] = {"pair": self.pair_info(symbol)["pair_key"], "type": side, "ordertype": "market", "volume": str(quantity)}
        if leverage is not None:
            payload["leverage"] = str(leverage)
        if self.dry_run:
            order_id = f"dry-{uuid.uuid4()}"
            return {"order_id": order_id, "status": "simulated", "symbol": symbol.upper(), "side": side, "requested_quantity": str(quantity), "executed_quantity": "0", "leverage": leverage, "dry_run": True, "payload": payload}
        result = self._signed("POST", "/0/private/AddOrder", payload)
        order_id = (result.get("txid") or [None])[0]
        return {"order_id": order_id, "status": "pending", "symbol": symbol.upper(), "side": side, "requested_quantity": str(quantity), "executed_quantity": "0", "leverage": leverage, "dry_run": False, "raw_result": result}

    def place_exit_limit(self, symbol: str, side: str, quantity: float | str, price: float | str) -> dict[str, Any]:
        return self._place_price_order(symbol, side, quantity, "limit", price)

    def place_stop_loss(self, symbol: str, side: str, quantity: float | str, stop_price: float | str) -> dict[str, Any]:
        return self._place_price_order(symbol, side, quantity, "stop-loss", stop_price)

    def _place_price_order(self, symbol: str, side: str, quantity: float | str, ordertype: str, price: float | str) -> dict[str, Any]:
        side = side.strip().lower()
        if side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        payload = {"pair": self.pair_info(symbol)["pair_key"], "type": side, "ordertype": ordertype, "volume": str(quantity), "price": str(price)}
        if self.dry_run:
            return {"order_id": f"dry-{uuid.uuid4()}", "status": "simulated", "dry_run": True, "payload": payload}
        result = self._signed("POST", "/0/private/AddOrder", payload)
        return {"order_id": (result.get("txid") or [None])[0], "status": "pending", "dry_run": False, "raw_result": result}

    def get_order(self, symbol: str, order_id: str) -> dict[str, Any]:
        if order_id.startswith("dry-"):
            return {"order_id": order_id, "status": "simulated", "dry_run": True}
        rows = self._signed("POST", "/0/private/QueryOrders", {"txid": order_id, "trades": True})
        row = rows.get(order_id, {})
        status = str(row.get("status") or "unknown").lower()
        return {"order_id": order_id, "status": "filled" if status == "closed" else status, "symbol": symbol.upper(), "side": row.get("type"), "requested_quantity": str(row.get("vol") or 0), "executed_quantity": str(row.get("vol_exec") or 0), "average_price": float(row.get("price") or 0), "dry_run": False, "raw_result": row}

    def open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        if self.dry_run:
            return []
        rows = (self._signed("POST", "/0/private/OpenOrders", {"trades": True}) or {}).get("open", {})
        return [{"order_id": oid, **row} for oid, row in rows.items()]

    def open_margin_positions(self) -> dict[str, Any]:
        if self.dry_run:
            return {}
        return self._signed("POST", "/0/private/OpenPositions", {"docalcs": "true"})

    def cancel_order(self, symbol: str, order_id: str) -> dict[str, Any]:
        if self.dry_run or order_id.startswith("dry-"):
            return {"order_id": order_id, "symbol": symbol.upper(), "status": "canceled", "dry_run": True}
        result = self._signed("POST", "/0/private/CancelOrder", {"txid": order_id})
        return {"order_id": order_id, "symbol": symbol.upper(), "status": "canceled", "dry_run": False, "raw_result": result}

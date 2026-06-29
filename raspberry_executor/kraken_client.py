from __future__ import annotations

import base64
import hashlib
import hmac
import time
from typing import Any
from urllib.parse import urlencode

import requests


class KrakenClient:
    """Kraken Spot REST adapter with the BinanceClient surface used by SignalMaker."""

    exchange_name = "kraken"

    def __init__(self, base_url: str, api_key: str, secret_key: str, dry_run: bool = True) -> None:
        self.base_url = (base_url or "https://api.kraken.com").rstrip("/")
        self.api_key = api_key
        self.secret_key = secret_key
        self.dry_run = dry_run
        self.session = requests.Session()
        self._asset_pair_cache: dict[str, dict[str, Any]] = {}

    def is_configured(self) -> bool:
        return bool(self.api_key and self.secret_key)

    def recv_window_ms(self) -> int:
        # Compatibility with BinanceClient; Kraken private requests use nonces
        # instead of recvWindow.
        return 0

    def _normalize_symbol(self, symbol: str) -> str:
        return symbol.upper().replace("/", "")

    def _symbol_aliases(self, symbol: str) -> set[str]:
        normalized = self._normalize_symbol(symbol)
        aliases = {normalized}
        if "BTC" in normalized:
            aliases.add(normalized.replace("BTC", "XBT"))
        if "XBT" in normalized:
            aliases.add(normalized.replace("XBT", "BTC"))
        return aliases

    def _asset_name(self, asset: str) -> str:
        value = str(asset or "").upper()
        if value in {"XBT", "XXBT"}:
            return "BTC"
        return value.lstrip("XZ")

    def _public(self, path: str, params: dict[str, Any] | None = None) -> Any:
        response = self.session.get(f"{self.base_url}{path}", params=params or {}, timeout=20)
        response.raise_for_status()
        data = response.json()
        errors = data.get("error") or []
        if errors:
            raise RuntimeError(f"Kraken GET {path} failed errors={errors}")
        return data.get("result")

    def _signed(self, method: str, path: str, params: dict[str, Any] | None = None) -> Any:
        if method.upper() != "POST":
            raise RuntimeError("Kraken private REST endpoints require POST")
        if not self.is_configured():
            raise RuntimeError("Kraken API credentials are missing")
        payload = dict(params or {})
        payload["nonce"] = str(int(time.time() * 1000))
        encoded = urlencode(payload)
        sha = hashlib.sha256((payload["nonce"] + encoded).encode()).digest()
        secret = base64.b64decode(self.secret_key)
        signature = hmac.new(secret, path.encode() + sha, hashlib.sha512).digest()
        headers = {"API-Key": self.api_key, "API-Sign": base64.b64encode(signature).decode()}
        response = self.session.post(f"{self.base_url}{path}", data=payload, headers=headers, timeout=20)
        response.raise_for_status()
        data = response.json()
        errors = data.get("error") or []
        if errors:
            raise RuntimeError(f"Kraken POST {path} failed errors={errors}")
        return data.get("result") or {}

    def _pair_info(self, symbol: str) -> dict[str, Any]:
        symbol = self._normalize_symbol(symbol)
        if symbol in self._asset_pair_cache:
            return self._asset_pair_cache[symbol]
        rows = self._public("/0/public/AssetPairs", {"assetVersion": 1}) or {}
        requested_aliases = self._symbol_aliases(symbol)
        for key, row in rows.items():
            altname = str(row.get("altname") or key).upper().replace("/", "")
            wsname = str(row.get("wsname") or "").upper().replace("/", "")
            kraken_aliases = self._symbol_aliases(altname) | self._symbol_aliases(wsname)
            if requested_aliases & kraken_aliases:
                base = str(row.get("base") or "").upper()
                quote = str(row.get("quote") or "").upper()
                info = {**row, "pair_key": key, "symbol": symbol, "baseAsset": self._asset_name(base), "quoteAsset": self._asset_name(quote)}
                self._asset_pair_cache[symbol] = info
                return info
        raise RuntimeError(f"kraken_pair_not_found:{symbol}")

    def _pair_key(self, symbol: str) -> str:
        return str(self._pair_info(symbol)["pair_key"])

    def current_price(self, symbol: str) -> float:
        result = self._public("/0/public/Ticker", {"pair": self._pair_key(symbol)}) or {}
        row = next(iter(result.values()))
        return float((row.get("c") or [0])[0])

    def account(self) -> dict:
        return self._signed("POST", "/0/private/Balance")

    def free_balance(self, asset: str) -> float:
        if self.dry_run:
            return 0.0
        balances = self.account()
        wanted = asset.upper()
        aliases = {wanted, f"X{wanted}", f"Z{wanted}"}
        if wanted == "BTC":
            aliases.add("XBT")
            aliases.add("XXBT")
        for key, value in balances.items():
            if str(key).upper() in aliases:
                return float(value or 0)
        return 0.0

    @staticmethod
    def average_fill_price(order_payload: dict[str, Any], fallback: float | None = None) -> float | None:
        descr = order_payload.get("descr") if isinstance(order_payload.get("descr"), dict) else {}
        price = order_payload.get("price") or descr.get("price")
        try:
            if price and float(price) > 0:
                return float(price)
        except Exception:
            pass
        return fallback

    def place_market_entry(self, symbol: str, side: str, quantity: float | str) -> dict:
        side_lower = "buy" if side.lower() == "long" else "sell"
        if self.dry_run:
            price = self.current_price(symbol)
            return {"orderId": f"dry-entry-{int(time.time())}", "txid": [f"dry-entry-{int(time.time())}"], "status": "FILLED", "symbol": symbol.upper(), "side": side_lower.upper(), "executedQty": str(quantity), "fills": [{"price": str(price), "qty": str(quantity)}], "dry_run": True}
        result = self._signed("POST", "/0/private/AddOrder", {"pair": self._pair_key(symbol), "type": side_lower, "ordertype": "market", "volume": quantity})
        txid = (result.get("txid") or [None])[0]
        return {"orderId": txid, "txid": result.get("txid"), "status": "NEW", "symbol": symbol.upper(), "side": side_lower.upper(), "executedQty": "0", "result": result}

    def place_exit_limit(self, symbol: str, side: str, quantity: float | str, price: float | str) -> dict:
        exit_side = "sell" if side.lower() == "long" else "buy"
        if self.dry_run:
            return {"orderId": f"dry-tp-{int(time.time())}", "status": "NEW", "price": str(price), "dry_run": True}
        result = self._signed("POST", "/0/private/AddOrder", {"pair": self._pair_key(symbol), "type": exit_side, "ordertype": "limit", "volume": quantity, "price": price})
        txid = (result.get("txid") or [None])[0]
        return {"orderId": txid, "txid": result.get("txid"), "status": "NEW", "price": str(price), "result": result}

    def place_stop_loss(self, symbol: str, side: str, quantity: float | str, stop_price: float | str) -> dict:
        exit_side = "sell" if side.lower() == "long" else "buy"
        if self.dry_run:
            return {"orderId": f"dry-sl-{int(time.time())}", "status": "NEW", "stopPrice": str(stop_price), "dry_run": True}
        result = self._signed("POST", "/0/private/AddOrder", {"pair": self._pair_key(symbol), "type": exit_side, "ordertype": "stop-loss", "volume": quantity, "price": stop_price})
        txid = (result.get("txid") or [None])[0]
        return {"orderId": txid, "txid": result.get("txid"), "status": "NEW", "stopPrice": str(stop_price), "result": result}

    def get_order(self, symbol: str, order_id: str | int) -> dict:
        if str(order_id).startswith("dry-"):
            return {"orderId": order_id, "status": "NEW", "dry_run": True}
        rows = self._signed("POST", "/0/private/QueryOrders", {"txid": str(order_id), "trades": True})
        row = rows.get(str(order_id), {}) if isinstance(rows, dict) else {}
        status = str(row.get("status") or "").upper()
        volume = row.get("vol_exec") or row.get("vol") or "0"
        return {"orderId": order_id, "status": "FILLED" if status == "CLOSED" else status, "symbol": symbol.upper(), "side": str(row.get("type") or "").upper(), "executedQty": str(volume), **row}

    def open_orders(self, symbol: str) -> list[dict]:
        if self.dry_run:
            return []
        rows = (self._signed("POST", "/0/private/OpenOrders", {"trades": True}) or {}).get("open") or {}
        pair = self._pair_key(symbol)
        return [{"orderId": oid, **row} for oid, row in rows.items() if str(row.get("descr", {}).get("pair") or "") in {pair, symbol.upper()}]

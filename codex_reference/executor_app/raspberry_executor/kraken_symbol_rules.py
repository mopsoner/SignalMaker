from __future__ import annotations

import math
from typing import Any

import requests


class KrakenSymbolRules:
    """Kraken Spot symbol metadata adapter matching KrakenSymbolRules methods."""

    def __init__(self, base_url: str, quote_assets: list[str] | None = None) -> None:
        self.base_url = (base_url or "https://api.kraken.com").rstrip("/")
        self.quote_assets = [q.upper() for q in (quote_assets or ["USDC"])]
        self._cache: dict[str, dict[str, Any]] = {}

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

    def symbol_info(self, symbol: str) -> dict[str, Any]:
        symbol = self._normalize_symbol(symbol)
        if symbol in self._cache:
            return self._cache[symbol]
        requested_aliases = self._symbol_aliases(symbol)
        response = requests.get(f"{self.base_url}/0/public/AssetPairs", params={"assetVersion": 1}, timeout=20)
        response.raise_for_status()
        data = response.json()
        errors = data.get("error") or []
        if errors:
            raise RuntimeError(f"Kraken AssetPairs failed errors={errors}")
        for key, row in (data.get("result") or {}).items():
            altname = str(row.get("altname") or key).upper().replace("/", "")
            wsname = str(row.get("wsname") or "").upper().replace("/", "")
            kraken_aliases = self._symbol_aliases(altname) | self._symbol_aliases(wsname)
            if requested_aliases & kraken_aliases:
                base = str(row.get("base") or "").upper()
                quote = str(row.get("quote") or "").upper()
                info = {**row, "pair_key": key, "symbol": symbol, "baseAsset": self._asset_name(base), "quoteAsset": self._asset_name(quote)}
                self._cache[symbol] = info
                return info
        raise RuntimeError(f"symbol_not_found:{symbol}")

    def base_asset(self, symbol: str) -> str:
        info = self.symbol_info(symbol)
        base = str(info.get("base") or "").upper()
        return self._asset_name(base)

    def _floor_decimals(self, value: float, decimals: int) -> str:
        factor = 10**max(decimals, 0)
        floored = math.floor(float(value) * factor) / factor
        return f"{floored:.{max(decimals, 0)}f}".rstrip("0").rstrip(".") or "0"

    def quantity_from_quote(self, symbol: str, quote_amount: float, current_price: float, *, market: bool = True) -> str:
        info = self.symbol_info(symbol)
        qty = self._floor_decimals(float(quote_amount) / float(current_price), int(info.get("lot_decimals", 8) or 8))
        self.ensure_exit_notional(symbol, qty, current_price, label="market_entry")
        return qty

    def normalize_exit_quantity(self, symbol: str, quantity: float | str) -> str:
        return self._floor_decimals(float(quantity), int(self.symbol_info(symbol).get("lot_decimals", 8) or 8))

    def normalize_market_quantity(self, symbol: str, quantity: float | str) -> str:
        return self.normalize_exit_quantity(symbol, quantity)

    def normalize_exit_price(self, symbol: str, price: float) -> str:
        return self._floor_decimals(float(price), int(self.symbol_info(symbol).get("pair_decimals", 8) or 8))

    def ensure_exit_notional(self, symbol: str, quantity: str, price: float | str, *, label: str) -> None:
        info = self.symbol_info(symbol)
        qty = float(quantity)
        ordermin = float(info.get("ordermin") or 0)
        if ordermin and qty < ordermin:
            raise RuntimeError(f"{label}_quantity_below_kraken_ordermin symbol={symbol} quantity={quantity} ordermin={ordermin}")
        costmin = float(info.get("costmin") or 0)
        notional = qty * float(price)
        if costmin and notional < costmin:
            raise RuntimeError(f"{label}_notional_below_kraken_costmin symbol={symbol} notional={notional} costmin={costmin}")

    def oco_allowed(self, symbol: str) -> bool:
        return False

from __future__ import annotations

from decimal import Decimal, ROUND_DOWN
from typing import Any

from .kraken_client import KrakenClient


class KrakenSymbolRules:
    def __init__(self, client: KrakenClient, quote_assets: list[str] | None = None) -> None:
        self.client = client
        self.quote_assets = {item.upper() for item in (quote_assets or ["USD", "USDC"])}

    def symbol_info(self, symbol: str) -> dict[str, Any]:
        info = self.client.pair_info(symbol)
        if info["quoteAsset"] not in self.quote_assets:
            raise ValueError(f"unsupported quote asset: {info['quoteAsset']}")
        return info

    def base_asset(self, symbol: str) -> str:
        return str(self.symbol_info(symbol)["baseAsset"])

    def quote_asset(self, symbol: str) -> str:
        return str(self.symbol_info(symbol)["quoteAsset"])

    @staticmethod
    def _floor(value: Decimal, decimals: int) -> str:
        quant = Decimal(1).scaleb(-max(decimals, 0))
        return format(value.quantize(quant, rounding=ROUND_DOWN), "f")

    def normalize_market_quantity(self, symbol: str, quantity: float | str | Decimal) -> str:
        result = self._floor(Decimal(str(quantity)), int(self.symbol_info(symbol).get("lot_decimals", 8)))
        if Decimal(result) <= 0:
            raise ValueError("normalized quantity is zero")
        return result

    normalize_exit_quantity = normalize_market_quantity

    def normalize_exit_price(self, symbol: str, price: float | str | Decimal) -> str:
        return self._floor(Decimal(str(price)), int(self.symbol_info(symbol).get("pair_decimals", 8)))

    def quantity_from_quote(self, symbol: str, quote_amount: float, current_price: float) -> str:
        quantity = self.normalize_market_quantity(symbol, Decimal(str(quote_amount)) / Decimal(str(current_price)))
        self.ensure_notional(symbol, quantity, current_price)
        return quantity

    def ensure_notional(self, symbol: str, quantity: float | str, price: float | str) -> None:
        info = self.symbol_info(symbol)
        qty = Decimal(str(quantity))
        if qty < Decimal(str(info.get("ordermin") or 0)):
            raise ValueError("quantity below Kraken order minimum")
        if qty * Decimal(str(price)) < Decimal(str(info.get("costmin") or 0)):
            raise ValueError("notional below Kraken cost minimum")

    def supported_leverages(self, symbol: str, side: str) -> tuple[int, ...]:
        normalized_side = side.strip().lower()
        if normalized_side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        key = f"leverage_{normalized_side}"
        supported: set[int] = set()
        for value in self.symbol_info(symbol).get(key, []):
            try:
                parsed = int(value)
            except (TypeError, ValueError):
                continue
            if parsed > 1:
                supported.add(parsed)
        return tuple(sorted(supported))

    def max_supported_leverage(self, symbol: str, side: str, configured_max: int) -> int:
        if configured_max < 2:
            raise ValueError("configured maximum leverage must be at least 2")
        eligible = tuple(value for value in self.supported_leverages(symbol, side) if value <= configured_max)
        if not eligible:
            raise ValueError(
                f"no Kraken margin leverage is supported for {symbol} {side} "
                f"within configured maximum {configured_max}"
            )
        return eligible[-1]

    def validate_leverage(self, symbol: str, side: str, leverage: int) -> int:
        supported = set(self.supported_leverages(symbol, side))
        if leverage not in supported:
            raise ValueError(f"leverage {leverage} is not supported for {symbol} {side}")
        return leverage

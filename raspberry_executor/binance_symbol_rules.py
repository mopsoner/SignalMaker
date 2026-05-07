import requests

from raspberry_executor.binance_filters import ensure_min_notional, normalize_price, normalize_quantity


class BinanceSymbolRules:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._cache: dict[str, dict] = {}

    def symbol_info(self, symbol: str) -> dict:
        symbol = symbol.upper()
        if symbol in self._cache:
            return self._cache[symbol]
        response = requests.get(
            f"{self.base_url}/api/v3/exchangeInfo",
            params={"symbol": symbol},
            timeout=20,
        )
        response.raise_for_status()
        rows = response.json().get("symbols") or []
        if not rows:
            raise RuntimeError(f"symbol_not_found:{symbol}")
        self._cache[symbol] = rows[0]
        return rows[0]

    def quantity_from_quote(self, symbol: str, quote_amount: float, current_price: float, *, market: bool = True) -> str:
        info = self.symbol_info(symbol)
        quantity = normalize_quantity(symbol.upper(), info, float(quote_amount) / float(current_price), market=market)
        ensure_min_notional(symbol.upper(), info, quantity, current_price, label="market_entry")
        return quantity

    def normalize_exit_quantity(self, symbol: str, quantity: float | str) -> str:
        info = self.symbol_info(symbol)
        return normalize_quantity(symbol.upper(), info, quantity, market=False)

    def normalize_exit_price(self, symbol: str, price: float) -> str:
        info = self.symbol_info(symbol)
        return normalize_price(symbol.upper(), info, price)

    def ensure_exit_notional(self, symbol: str, quantity: str, price: float | str, *, label: str) -> None:
        info = self.symbol_info(symbol)
        ensure_min_notional(symbol.upper(), info, quantity, price, label=label)

    def oco_allowed(self, symbol: str) -> bool:
        return bool(self.symbol_info(symbol).get("ocoAllowed", False))

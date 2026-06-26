from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from .errors import IBKRNoDataError


@dataclass
class IBKRCandle:
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    adjusted_close: Optional[Decimal]
    volume: Optional[Decimal]


class IBKRHistoricalService:
    def __init__(self, client, config):
        self.client = client
        self.config = config

    async def resolve_stock_conid(self, symbol: str, exchange: str | None = None) -> int:
        data = await self.client.get_json("trsrv/stocks", {"symbols": symbol.upper()})
        rows = data.get(symbol.upper()) if isinstance(data, dict) else None
        if not rows:
            raise IBKRNoDataError(f"No IBKR stock contract for {symbol}")
        wanted_exchange = (exchange or self.config.default_exchange or "").upper()
        for row in rows:
            for contract in row.get("contracts") or []:
                if wanted_exchange in {"", "SMART"} or str(contract.get("exchange") or "").upper() == wanted_exchange:
                    return int(contract["conid"])
        first_contract = (rows[0].get("contracts") or [None])[0]
        if not first_contract:
            raise IBKRNoDataError(f"No IBKR conid for {symbol}")
        return int(first_contract["conid"])

    async def fetch_daily_candles(self, provider_symbol: str) -> list[IBKRCandle]:
        symbol, _, exchange = provider_symbol.partition("@")
        conid = int(symbol) if symbol.isdigit() else await self.resolve_stock_conid(symbol, exchange or None)
        data = await self.client.get_json(
            "iserver/marketdata/history",
            {
                "conid": conid,
                "period": self.config.history_period,
                "bar": self.config.history_bar,
                "outsideRth": str(not self.config.use_regular_trading_hours).lower(),
            },
        )
        rows = data.get("data") if isinstance(data, dict) else data
        if not rows:
            raise IBKRNoDataError(f"No IBKR historical data for {provider_symbol}")
        candles: list[IBKRCandle] = []
        for row in rows:
            ts = row.get("t") or row.get("time") or row.get("date")
            dt = self._parse_timestamp(ts)
            candles.append(IBKRCandle(
                timestamp=dt.replace(tzinfo=None),
                open=Decimal(str(row.get("o") if row.get("o") is not None else row.get("open"))),
                high=Decimal(str(row.get("h") if row.get("h") is not None else row.get("high"))),
                low=Decimal(str(row.get("l") if row.get("l") is not None else row.get("low"))),
                close=Decimal(str(row.get("c") if row.get("c") is not None else row.get("close"))),
                adjusted_close=None,
                volume=Decimal(str(row.get("v") if row.get("v") is not None else row.get("volume"))) if (row.get("v") is not None or row.get("volume") is not None) else None,
            ))
        return candles

    def _parse_timestamp(self, value) -> datetime:
        if isinstance(value, (int, float)):
            seconds = float(value) / 1000 if value > 10_000_000_000 else float(value)
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        text = str(value)
        for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y%m%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(text[:len(datetime(2000, 1, 2, 3, 4, 5).strftime(fmt))], fmt)
            except ValueError:
                continue
        return datetime.fromisoformat(text.replace("Z", "+00:00"))

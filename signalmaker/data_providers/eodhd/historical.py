from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from .errors import EODHDNoDataError


@dataclass
class EODHDCandle:
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    adjusted_close: Optional[Decimal]
    volume: Optional[Decimal]


class EODHDHistoricalService:
    def __init__(self, client, config):
        self.client = client
        self.config = config

    async def fetch_daily_candles(self, provider_symbol: str) -> list[EODHDCandle]:
        data = await self.client.get_json(
            f"eod/{provider_symbol}",
            params={"from": self.config.start_date, "period": "d"},
        )
        if not data:
            raise EODHDNoDataError(f"No EODHD historical data for {provider_symbol}")
        candles: list[EODHDCandle] = []
        for row in data:
            candles.append(EODHDCandle(
                timestamp=datetime.strptime(row["date"], "%Y-%m-%d"),
                open=Decimal(str(row["open"])), high=Decimal(str(row["high"])),
                low=Decimal(str(row["low"])), close=Decimal(str(row["close"])),
                adjusted_close=Decimal(str(row["adjusted_close"])) if row.get("adjusted_close") is not None else None,
                volume=Decimal(str(row["volume"])) if row.get("volume") is not None else None,
            ))
        return candles

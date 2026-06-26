from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from .errors import IBKRHistoricalDataError


@dataclass
class HistoricalCandle:
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Optional[Decimal]


class IBKRHistoricalDataService:
    def __init__(self, ib, config):
        self.ib = ib
        self.config = config

    async def fetch_daily_candles(self, contract) -> list[HistoricalCandle]:
        try:
            bars = await self.ib.reqHistoricalDataAsync(
                contract,
                endDateTime="",
                durationStr=self.config.duration,
                barSizeSetting=self.config.bar_size,
                whatToShow=self.config.what_to_show,
                useRTH=self.config.use_rth,
                formatDate=1,
                keepUpToDate=False,
            )
        except Exception as exc:
            raise IBKRHistoricalDataError(
                f"Failed to fetch historical candles for {contract}"
            ) from exc

        candles: list[HistoricalCandle] = []

        for bar in bars:
            candles.append(
                HistoricalCandle(
                    timestamp=self._normalize_bar_date(bar.date),
                    open=Decimal(str(bar.open)),
                    high=Decimal(str(bar.high)),
                    low=Decimal(str(bar.low)),
                    close=Decimal(str(bar.close)),
                    volume=Decimal(str(bar.volume)) if bar.volume is not None else None,
                )
            )

        return candles

    def _normalize_bar_date(self, value) -> datetime:
        if isinstance(value, datetime):
            return value

        if isinstance(value, str):
            return datetime.strptime(value[:8], "%Y%m%d")

        raise ValueError(f"Unsupported IBKR bar date format: {value}")

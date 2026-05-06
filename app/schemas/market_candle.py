from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class MarketCandleRead(BaseModel):
    candle_id: str
    symbol: str
    interval: str
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    ingested_at: datetime

    model_config = ConfigDict(from_attributes=True)


class CandleSummary(BaseModel):
    symbol: str
    interval: str
    candle_count: int
    first_open: datetime
    last_close: datetime
    span_hours: float
    last_ingested: datetime


class MarketCandleIn(BaseModel):
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float = 0.0
    number_of_trades: int = 0
    taker_buy_base_volume: float = 0.0
    taker_buy_quote_volume: float = 0.0


class CandleIngestRequest(BaseModel):
    symbol: str
    interval: str
    source: str = "raspberry"
    candles: list[MarketCandleIn] = Field(default_factory=list)


class CandleIngestResponse(BaseModel):
    status: str
    source: str
    symbol: str
    interval: str
    received: int
    upserted: int

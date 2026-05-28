from datetime import datetime

from pydantic import BaseModel


class MomentumRead(BaseModel):
    rank: int
    symbol: str
    price: float | None = None
    momentum_15m: float | None = None
    momentum_1h: float | None = None
    momentum_4h: float | None = None
    momentum_score: float
    classification: str
    rsi_15m: float | None = None
    rsi_1h: float | None = None
    rsi_4h: float | None = None
    change_15m: float | None = None
    change_1h: float | None = None
    change_4h: float | None = None
    ema_trend_15m: str
    ema_trend_1h: str
    ema_trend_4h: str
    updated_at: datetime | None = None
    data_quality: str

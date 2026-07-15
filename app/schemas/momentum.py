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
    momentum_delta_15m: float = 0.0
    momentum_delta_1h: float = 0.0
    momentum_delta_4h: float = 0.0
    momentum_acceleration_15m: float = 0.0
    momentum_acceleration_1h: float = 0.0
    momentum_acceleration_4h: float = 0.0
    momentum_acceleration: float = 0.0
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

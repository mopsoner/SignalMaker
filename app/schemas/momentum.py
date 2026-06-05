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
    trade_ready: bool = False
    trade_ready_status: str = "blocked_not_evaluated"
    trade_ready_reason: str = "Trade readiness was not evaluated."
    in_entry_pool: bool = False
    entry_rsi_timeframe: str = "1h"
    entry_rsi_min: float = 45.0
    entry_rsi_max: float = 55.0
    rsi_15m: float | None = None
    rsi_1h: float | None = None
    rsi_4h: float | None = None
    change_15m: float | None = None
    change_1h: float | None = None
    change_4h: float | None = None
    ema_trend_15m: str
    ema_trend_1h: str
    ema_trend_4h: str
    structure_15m_status: str = "unknown"
    structure_15m_bias: str = "neutral"
    mss_15m_bearish: bool = False
    bos_15m_bearish: bool = False
    bos_15m_bullish: bool = False
    last_swing_low_15m: float | None = None
    last_swing_high_15m: float | None = None
    structure_broken_at: datetime | None = None
    structure_reason: str = "structure_not_calculated"
    updated_at: datetime | None = None
    calculated_at: datetime | None = None
    data_quality: str


class MomentumCandidateRead(MomentumRead):
    candidate_type: str = "momentum_trade_ready"

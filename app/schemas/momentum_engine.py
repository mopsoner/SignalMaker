from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class MomentumEnginePositionRead(BaseModel):
    position_id: str
    symbol: str
    status: str
    quantity: float
    entry_price: float
    entry_value: float
    entry_score: float | None = None
    entry_rank: int | None = None
    mark_price: float | None = None
    unrealized_pnl: float | None = None
    opened_at: datetime
    closed_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class MomentumEngineTradeRead(BaseModel):
    trade_id: str
    action: str
    symbol: str
    price: float
    quantity: float
    value: float
    pnl: float
    reason: str | None = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class MomentumEngineStatus(BaseModel):
    strategy: str
    mode: str = "paper"
    cadence_hours: int
    starting_capital: float
    cash: float
    equity: float
    total_pnl: float
    total_pnl_pct: float
    open_position: MomentumEnginePositionRead | None = None
    best_asset: dict | None = None
    last_check_at: datetime | None = None
    next_check_at: datetime | None = None
    due_now: bool
    recommendation: str
    trades: list[MomentumEngineTradeRead] = Field(default_factory=list)


class MomentumEngineRunRequest(BaseModel):
    force: bool = False
    cadence_hours: int = Field(default=4, ge=1, le=168)
    starting_capital: float = Field(default=1000.0, gt=0)
    min_momentum_score: float = 0.0

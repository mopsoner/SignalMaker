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
    mark_price_source: str | None = None
    unrealized_pnl: float | None = None
    structure_15m_status: str | None = None
    structure_15m_bias: str | None = None
    structure_reason: str | None = None
    structure_broken: bool | None = None
    support_status: str | None = None
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
    pnl_pct: float | None = None
    price_source: str | None = None
    reason: str | None = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class MomentumEngineDecision(BaseModel):
    strategy: str
    mode: str = "paper"
    cadence_hours: int
    starting_capital: float
    cash: float
    equity: float
    total_pnl: float
    total_pnl_pct: float
    action: str
    decision_action: str | None = None
    symbol: str | None = None
    target_symbol: str | None = None
    buy_symbol: str | None = None
    sell_symbol: str | None = None
    should_trade: bool | None = None
    status: str | None = None
    recommendation: str
    reason: str
    due_now: bool
    open_position: MomentumEnginePositionRead | None = None
    best_asset: dict | None = None
    top_watch_asset: dict | None = None
    last_check_at: datetime | None = None
    next_check_at: datetime | None = None
    produced_at: datetime | None = None
    source: str | None = None


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
    top_watch_asset: dict | None = None
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

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

MomentumDecisionAction = Literal["BUY", "SELL", "ROTATE", "WAIT", "HOLD"]
MomentumDecisionStatus = Literal["ready", "idle", "waiting", "skipped", "executed", "error"]

ALLOWED_MOMENTUM_DECISION_ACTIONS: set[str] = {"BUY", "SELL", "ROTATE", "WAIT", "HOLD"}
SUPPORTED_MOMENTUM_EXECUTOR_ACTIONS: set[str] = {"BUY", "SELL", "ROTATE", "WAIT", "HOLD"}
EXPECTED_MOMENTUM_DECISION_STATUSES: set[str] = {
    "ready",
    "idle",
    "waiting",
    "skipped",
    "executed",
    "error",
}


class MomentumDecision(BaseModel):
    decision_action: MomentumDecisionAction
    symbol: str | None = None
    target_symbol: str | None = None
    status: MomentumDecisionStatus
    reason: str
    order_ids: list[str] = Field(default_factory=list)
    fill_ids: list[str] = Field(default_factory=list)
    candidate_id: str | None = None
    side: str | None = None
    score: float | None = None
    entry_price: float | None = None
    target_price: float | None = None
    stop_price: float | None = None
    mark_price: float | None = None


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

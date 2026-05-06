from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field


class GatewayOrderReport(BaseModel):
    exchange_order_id: str | int | None = None
    status: str
    price: float | None = None
    avg_price: float | None = None
    executed_qty: float | None = None
    payload: dict[str, Any] | None = None


class GatewayExecutionReport(BaseModel):
    gateway_id: str = Field(min_length=1)
    candidate_id: str = Field(min_length=1)
    signal_symbol: str | None = None
    execution_symbol: str | None = None
    side: str
    quantity: float = Field(gt=0)
    entry_price: float | None = None
    stop_price: float | None = None
    target_price: float | None = None
    exchange: str = "binance"
    mode: str = "live"
    entry_order: GatewayOrderReport
    tp_order: GatewayOrderReport | None = None
    sl_order: GatewayOrderReport | None = None
    payload: dict[str, Any] | None = None


GatewayEventType = Literal[
    "entry_filled",
    "entry_rejected",
    "take_profit_filled",
    "stop_loss_filled",
    "position_closed",
    "order_cancelled",
    "execution_error",
]


class GatewayExecutionEvent(BaseModel):
    gateway_id: str = Field(min_length=1)
    candidate_id: str = Field(min_length=1)
    event_type: GatewayEventType
    exchange: str = "binance"
    exchange_order_id: str | int | None = None
    reason: str | None = None
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: dict[str, Any] | None = None


class GatewayHeartbeat(BaseModel):
    gateway_id: str = Field(min_length=1)
    status: str = "ok"
    version: str | None = None
    mode: str | None = None
    last_seen_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    meta: dict[str, Any] | None = None

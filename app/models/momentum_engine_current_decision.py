from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class MomentumEngineCurrentDecision(Base):
    __tablename__ = "momentum_engine_current_decision"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    decision_id: Mapped[str | None] = mapped_column(String(96), nullable=True)
    strategy: Mapped[str | None] = mapped_column(String(64), nullable=True)
    action: Mapped[str | None] = mapped_column(String(32), nullable=True)
    decision_action: Mapped[str | None] = mapped_column(String(32), nullable=True)
    symbol: Mapped[str | None] = mapped_column(String(32), nullable=True)
    target_symbol: Mapped[str | None] = mapped_column(String(32), nullable=True)
    buy_symbol: Mapped[str | None] = mapped_column(String(32), nullable=True)
    sell_symbol: Mapped[str | None] = mapped_column(String(32), nullable=True)
    should_trade: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    due_now: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    payload_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    produced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

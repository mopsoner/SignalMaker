from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class MomentumCurrent(Base):
    __tablename__ = "momentum_current"
    __table_args__ = (
        Index("ix_momentum_current_score", "momentum_score"),
        Index("ix_momentum_current_classification", "classification"),
        Index("ix_momentum_current_calculated_at", "calculated_at"),
    )

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    momentum_15m: Mapped[float | None] = mapped_column(Float, nullable=True)
    momentum_1h: Mapped[float | None] = mapped_column(Float, nullable=True)
    momentum_4h: Mapped[float | None] = mapped_column(Float, nullable=True)
    momentum_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    classification: Mapped[str] = mapped_column(String(32), default="neutral_bull", nullable=False)
    rsi_15m: Mapped[float | None] = mapped_column(Float, nullable=True)
    rsi_1h: Mapped[float | None] = mapped_column(Float, nullable=True)
    rsi_4h: Mapped[float | None] = mapped_column(Float, nullable=True)
    change_15m: Mapped[float | None] = mapped_column(Float, nullable=True)
    change_1h: Mapped[float | None] = mapped_column(Float, nullable=True)
    change_4h: Mapped[float | None] = mapped_column(Float, nullable=True)
    ema_trend_15m: Mapped[str] = mapped_column(String(32), default="unknown", nullable=False)
    ema_trend_1h: Mapped[str] = mapped_column(String(32), default="unknown", nullable=False)
    ema_trend_4h: Mapped[str] = mapped_column(String(32), default="unknown", nullable=False)
    data_quality: Mapped[str] = mapped_column(String(32), default="partial:0/3", nullable=False)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    calculated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

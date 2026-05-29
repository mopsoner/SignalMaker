from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, Float, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class MomentumStructureCurrent(Base):
    __tablename__ = "momentum_structure_current"
    __table_args__ = (
        Index("ix_momentum_structure_status", "structure_15m_status"),
        Index("ix_momentum_structure_bias", "structure_15m_bias"),
        Index("ix_momentum_structure_calculated_at", "calculated_at"),
    )

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    structure_15m_status: Mapped[str] = mapped_column(String(32), default="insufficient_data", nullable=False)
    structure_15m_bias: Mapped[str] = mapped_column(String(32), default="neutral", nullable=False)
    mss_15m_bearish: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    bos_15m_bearish: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    bos_15m_bullish: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    last_swing_low_15m: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_swing_high_15m: Mapped[float | None] = mapped_column(Float, nullable=True)
    structure_broken_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    structure_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    calculated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

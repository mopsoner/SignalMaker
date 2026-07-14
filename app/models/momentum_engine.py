from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, Index, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class MomentumEnginePosition(Base):
    __tablename__ = "momentum_engine_positions"
    __table_args__ = (
        Index("ix_momentum_engine_positions_status", "status"),
        Index("ix_momentum_engine_positions_symbol", "symbol"),
        Index("ix_momentum_engine_positions_strategy_status_opened", "strategy", "status", "opened_at"),
    )

    position_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    strategy: Mapped[str] = mapped_column(String(64), default="momentum_rotation_v1", nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="open", nullable=False)
    quantity: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    entry_price: Mapped[float] = mapped_column(Float, nullable=False)
    entry_value: Mapped[float] = mapped_column(Float, nullable=False)
    entry_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    entry_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mark_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    unrealized_pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    meta: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class MomentumEngineTrade(Base):
    __tablename__ = "momentum_engine_trades"
    __table_args__ = (
        Index("ix_momentum_engine_trades_action", "action"),
        Index("ix_momentum_engine_trades_symbol", "symbol"),
        Index("ix_momentum_engine_trades_created_at", "created_at"),
        Index("ix_momentum_engine_trades_strategy_created", "strategy", "created_at"),
        Index("ix_momentum_engine_trades_strategy_action", "strategy", "action"),
    )

    trade_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    strategy: Mapped[str] = mapped_column(String(64), default="momentum_rotation_v1", nullable=False)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    quantity: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    value: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    pnl: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    meta: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    @property
    def price_source(self) -> str | None:
        metadata = self.meta or {}
        source = metadata.get("price_source")
        return str(source) if source else None

    @property
    def pnl_pct(self) -> float | None:
        if not self.value:
            return None
        entry_value = float(self.value or 0.0) - float(self.pnl or 0.0)
        if entry_value <= 0:
            return None
        return round((float(self.pnl or 0.0) / entry_value) * 100, 4)

from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class MomentumBacktestRun(Base):
    __tablename__ = "momentum_backtest_runs"
    __table_args__ = (
        Index("ix_momentum_backtest_runs_status", "status"),
        Index("ix_momentum_backtest_runs_created_at", "created_at"),
    )

    run_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    status: Mapped[str] = mapped_column(String(32), default="queued", nullable=False)
    symbols_total: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    symbols_processed: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    initial_capital: Mapped[float] = mapped_column(Float, default=1000.0, nullable=False)
    final_equity: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_pnl_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    max_drawdown_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    trade_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    winrate: Mapped[float | None] = mapped_column(Float, nullable=True)
    profit_factor: Mapped[float | None] = mapped_column(Float, nullable=True)
    settings: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class MomentumBacktestTrade(Base):
    __tablename__ = "momentum_backtest_trades"
    __table_args__ = (
        Index("ix_momentum_backtest_trades_run", "run_id"),
        Index("ix_momentum_backtest_trades_symbol", "symbol"),
        Index("ix_momentum_backtest_trades_entry_time", "entry_time"),
    )

    trade_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(96), ForeignKey("momentum_backtest_runs.run_id"), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str] = mapped_column(String(16), default="long", nullable=False)
    entry_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    entry_price: Mapped[float] = mapped_column(Float, nullable=False)
    exit_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    exit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    entry_value: Mapped[float] = mapped_column(Float, nullable=False)
    exit_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    pnl_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    entry_rsi_1h: Mapped[float | None] = mapped_column(Float, nullable=True)
    exit_structure_15m: Mapped[str | None] = mapped_column(String(64), nullable=True)
    momentum_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class MomentumBacktestEquity(Base):
    __tablename__ = "momentum_backtest_equity"
    __table_args__ = (
        Index("ix_momentum_backtest_equity_run_time", "run_id", "timestamp"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    run_id: Mapped[str] = mapped_column(String(96), ForeignKey("momentum_backtest_runs.run_id"), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    equity: Mapped[float] = mapped_column(Float, nullable=False)
    cash: Mapped[float] = mapped_column(Float, nullable=False)
    position_symbol: Mapped[str | None] = mapped_column(String(32), nullable=True)
    position_value: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    drawdown_pct: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)

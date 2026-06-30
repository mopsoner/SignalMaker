from datetime import datetime, timezone

from sqlalchemy import BigInteger, DateTime, Float, Index, Integer, String, JSON
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class MarketCandle(Base):
    __tablename__ = "market_candles"
    __table_args__ = (
        Index("ix_market_candles_symbol_interval", "symbol", "interval"),
        Index("ix_market_candles_close_time", "close_time"),
    )

    candle_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    interval: Mapped[str] = mapped_column(String(16), nullable=False)
    open_time: Mapped[int] = mapped_column(BigInteger, nullable=False)
    close_time: Mapped[int] = mapped_column(BigInteger, nullable=False)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
    quote_volume: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    number_of_trades: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    taker_buy_base_volume: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    taker_buy_quote_volume: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    provider: Mapped[str] = mapped_column(String(32), nullable=False, default="KRAKEN")
    asset_id: Mapped[str | None] = mapped_column(String(96), nullable=True)
    provider_symbol: Mapped[str | None] = mapped_column(String(64), nullable=True)
    asset_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(16), nullable=True)
    exchange: Mapped[str | None] = mapped_column(String(64), nullable=True)
    universe: Mapped[str | None] = mapped_column(String(128), nullable=True)
    metadata_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

from sqlalchemy import Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column
from app.models.base import Base


class KrakenProfitContribution(Base):
    """Idempotency ledger: each confirmed position close contributes once."""
    __tablename__ = 'kraken_profit_contributions'
    position_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    day: Mapped[str] = mapped_column(String(10), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)


class KrakenDailyProfit(Base):
    __tablename__ = 'kraken_daily_profits'
    day: Mapped[str] = mapped_column(String(10), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    currency: Mapped[str] = mapped_column(String(8), primary_key=True)
    value: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    closed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

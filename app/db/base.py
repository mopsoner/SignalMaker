from sqlalchemy import text

from app.db.session import engine
from app.models.base import Base
from app.models.asset_state import AssetStateCurrent
from app.models.live_run import LiveRun
from app.models.trade_candidate import TradeCandidate
from app.models.position import Position
from app.models.market_candle import MarketCandle
from app.models.order import Order
from app.models.fill import Fill
from app.models.app_setting import AppSetting
from app.models.momentum_engine import MomentumEnginePosition, MomentumEngineTrade
from app.models.momentum_current import MomentumCurrent
from app.models.momentum_structure_current import MomentumStructureCurrent
from app.models.momentum_backtest import MomentumBacktestRun, MomentumBacktestTrade, MomentumBacktestEquity
from app.models.momentum_engine_current_decision import MomentumEngineCurrentDecision


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    _apply_compatible_schema_upgrades()


def _apply_compatible_schema_upgrades() -> None:
    """Apply small idempotent schema upgrades for deployments using create_all only."""
    if engine.dialect.name != "postgresql":
        return

    statements = (
        "ALTER TABLE momentum_engine_trades ALTER COLUMN action TYPE VARCHAR(64)",
        """ALTER TABLE momentum_current
            ADD COLUMN IF NOT EXISTS momentum_delta_15m DOUBLE PRECISION NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS momentum_delta_1h DOUBLE PRECISION NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS momentum_delta_4h DOUBLE PRECISION NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS momentum_acceleration_15m DOUBLE PRECISION NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS momentum_acceleration_1h DOUBLE PRECISION NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS momentum_acceleration_4h DOUBLE PRECISION NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS momentum_acceleration DOUBLE PRECISION NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS momentum_candle_time_15m TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS momentum_candle_time_1h TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS momentum_candle_time_4h TIMESTAMPTZ""",
    )
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))

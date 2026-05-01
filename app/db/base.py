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


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    _run_migrations()


def _run_migrations() -> None:
    from sqlalchemy import text
    batches = [
        [
            "SET LOCAL lock_timeout = '3s'",
            "ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS quote_volume FLOAT NOT NULL DEFAULT 0.0",
            "ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS number_of_trades INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS taker_buy_base_volume FLOAT NOT NULL DEFAULT 0.0",
            "ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS taker_buy_quote_volume FLOAT NOT NULL DEFAULT 0.0",
        ],
        [
            "SET LOCAL lock_timeout = '3s'",
            "ALTER TABLE asset_state_current ADD COLUMN IF NOT EXISTS rsi_15m FLOAT",
        ],
    ]
    for stmts in batches:
        try:
            with engine.begin() as conn:
                for stmt in stmts:
                    conn.execute(text(stmt))
        except Exception as exc:
            print(f"[migration] warning: {exc}")

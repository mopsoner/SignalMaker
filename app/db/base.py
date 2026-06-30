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


def init_db() -> None:
    Base.metadata.create_all(bind=engine)

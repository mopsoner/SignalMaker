from datetime import datetime, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.models.app_setting import AppSetting
from app.models.base import Base
from app.models.momentum_engine import MomentumEnginePosition, MomentumEngineTrade
from app.models.momentum_engine_current_decision import MomentumEngineCurrentDecision
from app.services.momentum_engine_service import MomentumEngineService
from app.services.momentum_market import STOCK_ETF_CONTEXT


def test_stock_etf_run_does_not_mutate_crypto_state_or_configuration() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    ranking = [{
        "symbol": "SPY", "rank": 1, "price": 500.0, "momentum_score": 10.0,
        "structure_15m_status": "valid", "mss_15m_bearish": False,
        "bos_15m_bearish": False,
    }]
    with Session(engine) as db:
        crypto_position = MomentumEnginePosition(
            position_id="crypto-position", market_scope="crypto",
            strategy=MomentumEngineService.STRATEGY, symbol="BTCUSDC", status="open",
            quantity=1, entry_price=100, entry_value=100,
            opened_at=datetime.now(timezone.utc),
        )
        crypto_trade = MomentumEngineTrade(
            trade_id="crypto-trade", market_scope="crypto",
            strategy=MomentumEngineService.STRATEGY, action="BUY", symbol="BTCUSDC",
            price=100, quantity=1, value=100, pnl=0,
        )
        crypto_decision = MomentumEngineCurrentDecision(
            id=1, market_scope="crypto", action="hold", payload_json={"action": "hold"},
        )
        crypto_config = AppSetting(category="momentum", key="momentum_engine_cadence_hours", value=1)
        db.add_all([crypto_position, crypto_trade, crypto_decision, crypto_config])
        db.commit()

        service = MomentumEngineService(
            db, context=STOCK_ETF_CONTEXT, ranking_loader=lambda _limit: ranking,
            candle_loader=lambda _symbol: (500.0, "test"),
        )
        service.run_once(force=True, cadence_hours=24, starting_capital=10_000)

        assert db.get(MomentumEnginePosition, "crypto-position").status == "open"
        assert db.get(MomentumEngineTrade, "crypto-trade").action == "BUY"
        assert db.get(MomentumEngineCurrentDecision, 1).payload_json == {"action": "hold"}
        assert db.scalar(select(AppSetting.value).where(AppSetting.category == "momentum")) == 1
        assert db.scalar(select(MomentumEnginePosition).where(MomentumEnginePosition.market_scope == "stock_etf")) is not None
        assert db.scalar(select(MomentumEngineCurrentDecision).where(MomentumEngineCurrentDecision.market_scope == "stock_etf")) is not None

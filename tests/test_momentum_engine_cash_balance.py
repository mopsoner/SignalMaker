from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.models.base import Base
from app.models.market_candle import MarketCandle
from app.models.momentum_current import MomentumCurrent
from app.models.momentum_engine import MomentumEnginePosition, MomentumEngineTrade
from app.models.momentum_structure_current import MomentumStructureCurrent
from app.services.momentum_engine_service import MomentumEngineService


def _make_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    return Session(engine)


def test_cash_balance_simulates_paper_cash_from_realized_pnl_and_open_position() -> None:
    with _make_session() as db:
        db.add_all(
            [
                MomentumEngineTrade(
                    trade_id="closed-sell",
                    strategy=MomentumEngineService.STRATEGY,
                    action="SELL_ROTATE_OR_STRUCTURE_BREAK",
                    symbol="BTCUSDC",
                    price=125.0,
                    quantity=10.0,
                    value=1250.0,
                    pnl=250.0,
                    created_at=datetime.now(timezone.utc),
                ),
                MomentumEngineTrade(
                    trade_id="check-1",
                    strategy=MomentumEngineService.STRATEGY,
                    action="HOLD_NO_NEXT_ENTRY",
                    symbol="BTCUSDC",
                    price=125.0,
                    quantity=0.0,
                    value=9999.0,
                    pnl=0.0,
                    created_at=datetime.now(timezone.utc),
                ),
                MomentumEngineTrade(
                    trade_id="other-strategy-sell",
                    strategy="other_strategy",
                    action="SELL_ROTATE_OR_STRUCTURE_BREAK",
                    symbol="ETHUSDC",
                    price=10.0,
                    quantity=10.0,
                    value=100.0,
                    pnl=500.0,
                    created_at=datetime.now(timezone.utc),
                ),
                MomentumEnginePosition(
                    position_id="open-1",
                    strategy=MomentumEngineService.STRATEGY,
                    symbol="SOLUSDC",
                    status="open",
                    quantity=10.0,
                    entry_price=100.0,
                    entry_value=1000.0,
                    opened_at=datetime.now(timezone.utc),
                ),
            ]
        )
        db.commit()

        cash = MomentumEngineService(db)._cash_balance(starting_capital=1000.0)

    assert cash == 250.0


def test_orphaned_buy_ledger_without_open_position_does_not_block_next_entry() -> None:
    with _make_session() as db:
        db.add_all(
            [
                MomentumEngineTrade(
                    trade_id="orphan-buy",
                    strategy=MomentumEngineService.STRATEGY,
                    action="BUY_RSI_1H_ENTRY_READY",
                    symbol="BTCUSDC",
                    price=100.0,
                    quantity=10.0,
                    value=1000.0,
                    pnl=0.0,
                    created_at=datetime.now(timezone.utc),
                ),
                MomentumCurrent(
                    symbol="ETHUSDC",
                    price=100.0,
                    momentum_score=10.0,
                    classification="strong_bull",
                    rsi_1h=50.0,
                    rank=1,
                    calculated_at=datetime.now(timezone.utc),
                ),
                MomentumStructureCurrent(
                    symbol="ETHUSDC",
                    structure_15m_status="valid",
                    structure_15m_bias="neutral_bullish",
                    structure_reason="15m_structure_holding_above_last_swing_low",
                    calculated_at=datetime.now(timezone.utc),
                ),
            ]
        )
        db.commit()

        status = MomentumEngineService(db).run_once(force=True, starting_capital=1000.0)
        actions = list(db.scalars(select(MomentumEngineTrade.action)).all())
        position = db.scalars(select(MomentumEnginePosition).where(MomentumEnginePosition.status == "open")).one()

    assert "CHECK_NO_CASH" not in actions
    assert "BUY_RSI_1H_ENTRY_READY" in actions
    assert position.symbol == "ETHUSDC"
    assert status["open_position"]["symbol"] == "ETHUSDC"


def test_hold_trade_records_mark_to_market_pnl_with_latest_market_price() -> None:
    with _make_session() as db:
        db.add_all(
            [
                MomentumCurrent(
                    symbol="BTCUSDC",
                    price=100.0,
                    momentum_score=10.0,
                    classification="strong_bull",
                    rsi_1h=70.0,
                    rank=1,
                    calculated_at=datetime.now(timezone.utc),
                ),
                MomentumStructureCurrent(
                    symbol="BTCUSDC",
                    structure_15m_status="valid",
                    structure_15m_bias="neutral_bullish",
                    structure_reason="15m_structure_holding_above_last_swing_low",
                    calculated_at=datetime.now(timezone.utc),
                ),
                MomentumEnginePosition(
                    position_id="pos-1",
                    strategy=MomentumEngineService.STRATEGY,
                    symbol="BTCUSDC",
                    status="open",
                    quantity=1.0,
                    entry_price=100.0,
                    entry_value=100.0,
                    opened_at=datetime.now(timezone.utc),
                ),
                MarketCandle(
                    candle_id="btc-15m-1",
                    symbol="BTCUSDC",
                    interval="15m",
                    open_time=1,
                    close_time=2,
                    open=124.0,
                    high=126.0,
                    low=123.0,
                    close=125.0,
                    volume=1.0,
                ),
                MarketCandle(
                    candle_id="btc-1h-1",
                    symbol="BTCUSDC",
                    interval="1h",
                    open_time=3,
                    close_time=4,
                    open=129.0,
                    high=131.0,
                    low=128.0,
                    close=130.0,
                    volume=1.0,
                ),
            ]
        )
        db.commit()

        MomentumEngineService(db).run_once(force=True, starting_capital=100.0)

        hold_trade = db.scalars(select(MomentumEngineTrade).where(MomentumEngineTrade.action == "HOLD_NO_NEXT_ENTRY")).one()
        position = db.get(MomentumEnginePosition, "pos-1")

    assert hold_trade.price == 130.0
    assert hold_trade.value == 130.0
    assert hold_trade.pnl == 30.0
    assert hold_trade.pnl_pct == 30.0
    assert hold_trade.price_source == "market_candle:1h"
    assert position is not None
    assert position.mark_price == 130.0
    assert position.unrealized_pnl == 30.0


def test_open_new_position_uses_latest_market_price_instead_of_stale_ranking_price() -> None:
    with _make_session() as db:
        db.add_all(
            [
                MarketCandle(
                    candle_id="eth-15m-1",
                    symbol="ETHUSDC",
                    interval="15m",
                    open_time=1,
                    close_time=2,
                    open=118.0,
                    high=121.0,
                    low=117.0,
                    close=120.0,
                    volume=1.0,
                ),
                MarketCandle(
                    candle_id="eth-4h-1",
                    symbol="ETHUSDC",
                    interval="4h",
                    open_time=3,
                    close_time=4,
                    open=128.0,
                    high=131.0,
                    low=127.0,
                    close=130.0,
                    volume=1.0,
                ),
            ]
        )
        db.commit()

        position = MomentumEngineService(db)._open_new_position(
            {
                "symbol": "ETHUSDC",
                "price": 100.0,
                "momentum_score": 12.0,
                "rank": 1,
                "classification": "strong_bull",
                "data_quality": "complete",
                "structure_15m_status": "valid",
                "structure_reason": "15m_structure_holding_above_last_swing_low",
                "rsi_1h": 50.0,
                "entry_status": "ready",
            },
            cash=240.0,
            action="BUY_RSI_1H_ENTRY_READY",
        )
        trade = db.scalars(select(MomentumEngineTrade).where(MomentumEngineTrade.action == "BUY_RSI_1H_ENTRY_READY")).one()

    assert position.entry_price == 130.0
    assert position.quantity == pytest.approx(240.0 / 130.0)
    assert trade.price == 130.0
    assert trade.price_source == "market_candle:4h"

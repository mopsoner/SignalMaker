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


def test_cash_balance_uses_buy_sell_trade_values() -> None:
    with _make_session() as db:
        db.add_all(
            [
                MomentumEngineTrade(
                    trade_id="buy-1",
                    strategy=MomentumEngineService.STRATEGY,
                    action="BUY_NEXT_ENTRY_READY",
                    symbol="BTCUSDC",
                    price=100.0,
                    quantity=5.0,
                    value=500.0,
                    pnl=0.0,
                    created_at=datetime.now(timezone.utc),
                ),
                MomentumEngineTrade(
                    trade_id="sell-1",
                    strategy=MomentumEngineService.STRATEGY,
                    action="SELL_ROTATE_OR_STRUCTURE_BREAK",
                    symbol="BTCUSDC",
                    price=125.0,
                    quantity=2.0,
                    value=250.0,
                    pnl=50.0,
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
                    trade_id="other-strategy-buy",
                    strategy="other_strategy",
                    action="BUY_NEXT_ENTRY_READY",
                    symbol="ETHUSDC",
                    price=10.0,
                    quantity=10.0,
                    value=100.0,
                    pnl=0.0,
                    created_at=datetime.now(timezone.utc),
                ),
            ]
        )
        db.commit()

        cash = MomentumEngineService(db)._cash_balance(starting_capital=1000.0)

    assert cash == 750.0


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


def _base_decision_status(**overrides: object) -> dict:
    status = {
        "strategy": MomentumEngineService.STRATEGY,
        "recommendation": "test recommendation",
        "due_now": True,
        "last_check_at": None,
        "next_check_at": None,
        "open_position": None,
        "best_asset": None,
    }
    status.update(overrides)
    return status


def test_executor_contract_buys_best_asset_when_due_without_position() -> None:
    from app.api.routes.momentum_engine import build_executor_contract

    best_asset = {"symbol": "ETHUSDC", "rank": 1}
    decision = build_executor_contract(_base_decision_status(best_asset=best_asset))

    assert decision["source"] == "momentum_engine_status"
    assert decision["action"] == "BUY"
    assert decision["symbol"] == "ETHUSDC"
    assert decision["buy_symbol"] == "ETHUSDC"
    assert decision["sell_symbol"] is None
    assert decision["should_trade"] is True
    assert decision["buy_candidates"] == [best_asset]
    assert decision["executor_contract"]["order_sequence"] == [{"type": "BUY", "symbol": "ETHUSDC"}]
    assert decision["status"]["best_asset"] == best_asset


def test_executor_contract_holds_when_not_due_with_open_position() -> None:
    from app.api.routes.momentum_engine import build_executor_contract

    decision = build_executor_contract(
        _base_decision_status(
            due_now=False,
            open_position={"symbol": "BTCUSDC", "structure_broken": True},
            best_asset={"symbol": "ETHUSDC"},
        )
    )

    assert decision["action"] == "HOLD"
    assert decision["symbol"] == "BTCUSDC"
    assert decision["buy_symbol"] is None
    assert decision["sell_symbol"] is None
    assert decision["should_trade"] is False
    assert decision["executor_contract"]["order_sequence"] == []


def test_executor_contract_rotates_broken_position_to_best_asset() -> None:
    from app.api.routes.momentum_engine import build_executor_contract

    decision = build_executor_contract(
        _base_decision_status(
            open_position={"symbol": "BTCUSDC", "structure_broken": True},
            best_asset={"symbol": "ETHUSDC"},
        )
    )

    assert decision["action"] == "ROTATE"
    assert decision["symbol"] == "ETHUSDC"
    assert decision["buy_symbol"] == "ETHUSDC"
    assert decision["sell_symbol"] == "BTCUSDC"
    assert decision["should_trade"] is True
    assert decision["executor_contract"]["order_sequence"] == [
        {"type": "SELL", "symbol": "BTCUSDC"},
        {"type": "BUY", "symbol": "ETHUSDC"},
    ]


def test_executor_contract_sells_broken_position_without_best_asset() -> None:
    from app.api.routes.momentum_engine import build_executor_contract

    decision = build_executor_contract(
        _base_decision_status(open_position={"symbol": "BTCUSDC", "structure_broken": True})
    )

    assert decision["action"] == "SELL"
    assert decision["symbol"] == "BTCUSDC"
    assert decision["buy_symbol"] is None
    assert decision["sell_symbol"] == "BTCUSDC"
    assert decision["should_trade"] is True
    assert decision["executor_contract"]["order_sequence"] == [{"type": "SELL", "symbol": "BTCUSDC"}]


def test_executor_contract_rotates_to_different_best_asset() -> None:
    from app.api.routes.momentum_engine import build_executor_contract

    decision = build_executor_contract(
        _base_decision_status(
            open_position={"symbol": "BTCUSDC", "structure_broken": False},
            best_asset={"symbol": "ETHUSDC"},
        )
    )

    assert decision["action"] == "ROTATE"
    assert decision["symbol"] == "ETHUSDC"
    assert decision["buy_symbol"] == "ETHUSDC"
    assert decision["sell_symbol"] == "BTCUSDC"
    assert decision["should_trade"] is True
    assert decision["executor_contract"]["order_sequence"] == [
        {"type": "SELL", "symbol": "BTCUSDC"},
        {"type": "BUY", "symbol": "ETHUSDC"},
    ]


def test_executor_contract_waits_when_not_due_without_position() -> None:
    from app.api.routes.momentum_engine import build_executor_contract

    decision = build_executor_contract(_base_decision_status(due_now=False, best_asset={"symbol": "ETHUSDC"}))

    assert decision["action"] == "WAIT"
    assert decision["symbol"] is None
    assert decision["buy_symbol"] is None
    assert decision["sell_symbol"] is None
    assert decision["should_trade"] is False
    assert decision["executor_contract"]["order_sequence"] == []

from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.api.routes import momentum_engine as momentum_engine_routes
from app.models.base import Base
from app.models.market_candle import MarketCandle
from app.models.momentum_current import MomentumCurrent
from app.models.momentum_engine import MomentumEnginePosition, MomentumEngineTrade
from app.models.momentum_engine_current_decision import MomentumEngineCurrentDecision
from app.models.momentum_structure_current import MomentumStructureCurrent
from app.services.momentum_engine_service import MomentumEngineService


def _make_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    return Session(engine)


def test_decision_endpoint_serializes_executor_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    produced_at = datetime(2026, 7, 11, 12, 30, tzinfo=timezone.utc)

    def fake_current_decision(self):  # type: ignore[no-untyped-def]
        return {
            "strategy": MomentumEngineService.STRATEGY,
            "mode": "paper",
            "cadence_hours": 4,
            "starting_capital": 1000.0,
            "cash": 1000.0,
            "equity": 1000.0,
            "total_pnl": 0.0,
            "total_pnl_pct": 0.0,
            "action": "buy",
            "decision_action": "buy",
            "symbol": "ETHUSDC",
            "target_symbol": "ETHUSDC",
            "buy_symbol": "ETHUSDC",
            "sell_symbol": None,
            "should_trade": True,
            "status": "trade_ready",
            "recommendation": "Buy ETHUSDC.",
            "reason": "Momentum asset ETHUSDC is entry-ready.",
            "due_now": True,
            "open_position": None,
            "best_asset": {"symbol": "ETHUSDC"},
            "top_watch_asset": None,
            "last_check_at": None,
            "next_check_at": None,
            "produced_at": produced_at,
            "source": "persisted_current_decision",
        }

    monkeypatch.setattr(MomentumEngineService, "current_decision", fake_current_decision)

    app = FastAPI()
    app.include_router(momentum_engine_routes.router, prefix="/api/v1/momentum-engine")
    app.dependency_overrides[get_db] = lambda: None

    response = TestClient(app).get("/api/v1/momentum-engine/decision")

    assert response.status_code == 200
    body = response.json()
    assert body["decision_action"] == "buy"
    assert body["buy_symbol"] == "ETHUSDC"
    assert body["sell_symbol"] is None
    assert body["should_trade"] is True
    assert body["status"] == "trade_ready"
    assert body["produced_at"] == "2026-07-11T12:30:00Z"
    assert body["source"] == "persisted_current_decision"


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


def test_decision_buy_does_not_write_trade() -> None:
    with _make_session() as db:
        db.add_all(
            [
                MomentumCurrent(
                    symbol="ETHUSDC",
                    price=100.0,
                    momentum_score=12.0,
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

        decision = MomentumEngineService(db).decision(starting_capital=1000.0)
        trades = list(db.scalars(select(MomentumEngineTrade)).all())
        positions = list(db.scalars(select(MomentumEnginePosition)).all())

    assert decision["action"] == "buy"
    assert decision["symbol"] == "ETHUSDC"
    assert decision["target_symbol"] == "ETHUSDC"
    assert decision["best_asset"]["entry_status"] == "ready"
    assert trades == []
    assert positions == []


def test_decision_hold_open_position_when_no_next_entry_ready() -> None:
    with _make_session() as db:
        db.add_all(
            [
                MomentumCurrent(
                    symbol="BTCUSDC",
                    price=100.0,
                    momentum_score=12.0,
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
            ]
        )
        db.commit()

        decision = MomentumEngineService(db).decision(starting_capital=100.0)
        trades = list(db.scalars(select(MomentumEngineTrade)).all())

    assert decision["action"] == "hold"
    assert decision["symbol"] == "BTCUSDC"
    assert decision["target_symbol"] == "BTCUSDC"
    assert decision["open_position"]["symbol"] == "BTCUSDC"
    assert trades == []


def test_current_decision_reads_persisted_current_snapshot_without_recomputing(monkeypatch: pytest.MonkeyPatch) -> None:
    persisted_payload = {
        "strategy": MomentumEngineService.STRATEGY,
        "mode": "paper",
        "cadence_hours": 4,
        "starting_capital": 1000.0,
        "cash": 1000.0,
        "equity": 1000.0,
        "total_pnl": 0.0,
        "total_pnl_pct": 0.0,
        "action": "buy",
        "symbol": "ETHUSDC",
        "target_symbol": "ETHUSDC",
        "recommendation": "latest",
        "reason": "latest persisted decision",
        "due_now": True,
    }

    def fail_if_recomputed(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("current_decision must not recompute decisions")

    monkeypatch.setattr(MomentumEngineService, "decision", fail_if_recomputed)
    monkeypatch.setattr(MomentumEngineService, "_rankings", fail_if_recomputed)
    monkeypatch.setattr(MomentumEngineService, "_build_status", fail_if_recomputed)
    monkeypatch.setattr(MomentumEngineService, "_decision_from_status", fail_if_recomputed)

    with _make_session() as db:
        db.add(
            MomentumEngineCurrentDecision(
                id=1,
                strategy=MomentumEngineService.STRATEGY,
                payload_json=persisted_payload,
                updated_at=datetime.now(timezone.utc),
            )
        )
        db.commit()

        assert MomentumEngineService(db).current_decision() == persisted_payload


def test_current_decision_returns_fallback_when_no_current_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_if_recomputed(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("current_decision must not recompute fallback decisions")

    monkeypatch.setattr(MomentumEngineService, "_rankings", fail_if_recomputed)
    monkeypatch.setattr(MomentumEngineService, "_build_status", fail_if_recomputed)
    monkeypatch.setattr(MomentumEngineService, "_decision_from_status", fail_if_recomputed)

    with _make_session() as db:
        fallback = MomentumEngineService(db).current_decision()

    assert fallback == {
        "strategy": MomentumEngineService.STRATEGY,
        "mode": "paper",
        "cadence_hours": 4,
        "starting_capital": 1000.0,
        "cash": 0.0,
        "equity": 0.0,
        "total_pnl": 0.0,
        "total_pnl_pct": 0.0,
        "action": "WAIT",
        "decision_action": "WAIT",
        "symbol": None,
        "target_symbol": None,
        "buy_symbol": None,
        "sell_symbol": None,
        "should_trade": False,
        "status": "no_persisted_decision",
        "recommendation": "No persisted momentum decision available yet.",
        "reason": "No persisted momentum decision available yet.",
        "due_now": False,
        "open_position": None,
        "best_asset": None,
        "top_watch_asset": None,
        "last_check_at": None,
        "next_check_at": None,
        "source": "persisted_current_decision",
    }


def test_current_decision_returns_fallback_when_current_snapshot_payload_is_empty() -> None:
    with _make_session() as db:
        db.add(
            MomentumEngineCurrentDecision(
                id=1,
                strategy=MomentumEngineService.STRATEGY,
                payload_json={},
                updated_at=datetime.now(timezone.utc),
            )
        )
        db.commit()

        fallback = MomentumEngineService(db).current_decision()

    assert fallback["status"] == "no_persisted_decision"
    assert fallback["action"] == "WAIT"
    assert fallback["source"] == "persisted_current_decision"


def test_rotation_flushes_closed_position_before_recomputing_cash_when_autoflush_disabled() -> None:
    with _make_session() as db:
        db.add_all(
            [
                MomentumCurrent(
                    symbol="BTCUSDC",
                    price=100.0,
                    momentum_score=10.0,
                    classification="bull",
                    rsi_1h=70.0,
                    rank=2,
                    calculated_at=datetime.now(timezone.utc),
                ),
                MomentumStructureCurrent(
                    symbol="BTCUSDC",
                    structure_15m_status="valid",
                    structure_15m_bias="neutral_bullish",
                    structure_reason="15m_structure_holding_above_last_swing_low",
                    calculated_at=datetime.now(timezone.utc),
                ),
                MomentumCurrent(
                    symbol="ETHUSDC",
                    price=50.0,
                    momentum_score=12.0,
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
                MomentumEnginePosition(
                    position_id="pos-btc",
                    strategy=MomentumEngineService.STRATEGY,
                    symbol="BTCUSDC",
                    status="open",
                    quantity=10.0,
                    entry_price=100.0,
                    entry_value=1000.0,
                    opened_at=datetime.now(timezone.utc),
                ),
            ]
        )
        db.commit()
        db.autoflush = False

        status = MomentumEngineService(db).run_once(force=True, starting_capital=1000.0)
        actions = list(db.scalars(select(MomentumEngineTrade.action)).all())
        open_position = db.scalars(select(MomentumEnginePosition).where(MomentumEnginePosition.status == "open")).one()

    assert "SELL_ROTATE_OR_STRUCTURE_BREAK" in actions
    assert "BUY_NEXT_ENTRY_READY" in actions
    assert "CHECK_NO_CASH" not in actions
    assert open_position.symbol == "ETHUSDC"
    assert status["open_position"]["symbol"] == "ETHUSDC"

# Retain regression coverage of the explicitly supported one-slot engine.
from app.services.momentum_market import MomentumMarketContext
SINGLE_POSITION_CRYPTO = MomentumMarketContext(market_scope="crypto", reference_currency="USDC", max_positions=1)

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from app.models.market_candle import MarketCandle
from app.models.momentum_engine import MomentumEngineTrade
from app.schemas.momentum_engine import MomentumEngineDecision
from app.services.momentum_engine_service import MomentumEngineService
from app.services.momentum_market import STOCK_ETF_CONTEXT
from test_momentum_engine_cash_balance import _make_session, _momentum_row, _open_engine_position


def add_candles(db, interval, closes, *, last_close=None):
    end = last_close or datetime.now(timezone.utc) - timedelta(minutes=1)
    width = timedelta(minutes=15) if interval == "15m" else timedelta(hours=1)
    for index, close in enumerate(closes):
        closed_at = end - width * (len(closes) - index - 1)
        db.add(MarketCandle(
            candle_id=f"BTCUSDC-{interval}-{index}", symbol="BTCUSDC", interval=interval,
            open_time=int((closed_at - width).timestamp() * 1000),
            close_time=int(closed_at.timestamp() * 1000),
            open=close, high=close, low=close, close=close, volume=1,
        ))


@pytest.mark.parametrize("interval", ["15m", "1h"])
@pytest.mark.parametrize("rsi,blocked", [(80, False), (80.01, True), (79.99, False)])
def test_rsi_threshold_no_longer_blocks_prealert_entries(interval, rsi, blocked):
    with _make_session() as db:
        rows = _momentum_row("BTCUSDC", price=100, momentum_score=30, stored_rank=1)
        setattr(rows[0], f"rsi_{interval}", rsi)
        db.add_all(rows)
        db.commit()
        service = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)
        decision = service.decision()
        assert decision["best_asset"]["symbol"] == "BTCUSDC"
        assert not service._exit_required(service._rankings()[0])


@pytest.mark.parametrize("interval", ["15m", "1h"])
@pytest.mark.parametrize("minutes_ago,blocked", [(30, True), (61, False)])
def test_downward_cross_uses_actual_close_time_and_expires_after_one_hour(interval, minutes_ago, blocked):
    with _make_session() as db:
        db.add_all(_momentum_row("BTCUSDC", price=100, momentum_score=30, stored_rank=1))
        # RSI moves from 100 to 72.22 on the final closed candle.
        add_candles(db, interval, list(range(100, 115)) + [109],
                    last_close=datetime.now(timezone.utc) - timedelta(minutes=minutes_ago))
        db.commit()
        asset = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)._rankings()[0]
        assert asset["rsi_recent_cross_down"] is blocked
        assert MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)._entry_valid(asset) is (interval != "15m")
        # A new service/session evaluation sees the same cross (no in-memory cooldown).
        assert MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)._rankings()[0]["rsi_recent_cross_down"] is blocked


def test_cross_is_still_detected_after_more_15m_candles():
    with _make_session() as db:
        db.add_all(_momentum_row("BTCUSDC", price=100, momentum_score=30, stored_rank=1))
        add_candles(db, "15m", list(range(100, 115)) + [109, 109.1, 109.2, 109.3])
        db.commit()
        asset = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)._rankings()[0]
        assert asset["rsi_recent_cross_down"] is True
        assert asset["closed_change_15m"] > 0
        assert MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)._support_status(asset) == "prealert_valid"


@pytest.mark.parametrize("replacement", [False, True])
def test_negative_15m_close_exits_before_cadence_and_does_not_repeat(replacement):
    with _make_session() as db:
        db.add_all(_momentum_row("BTCUSDC", price=100, momentum_score=30, stored_rank=1))
        db.add(_open_engine_position("BTCUSDC", entry_price=100, entry_value=1000, quantity=10, stored_rank=1))
        add_candles(db, "15m", [110, 100])
        if replacement:
            db.add_all(_momentum_row("ETHUSDC", price=50, momentum_score=20, stored_rank=2))
        db.commit()
        service = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)
        decision = service.decision()
        assert decision["cadence_due"] is False
        assert decision["event_due"] is True
        assert decision["due_reason"] == "negative_change_15m"
        assert decision["action"] == ("rotate" if replacement else "sell")
        assert MomentumEngineDecision.model_validate(decision).open_position.negative_15m_exit
        service.run_once()
        actions = list(db.scalars(select(MomentumEngineTrade.action)))
        assert "SELL_NEGATIVE_15M" in actions
        persisted = service.current_decision()
        assert persisted["action"] == ("rotate" if replacement else "sell")
        assert persisted["sell_symbol"] == "BTCUSDC"
        assert persisted["should_trade"] is True
        assert persisted["buy_symbol"] == ("ETHUSDC" if replacement else None)
        assert service._open_position().symbol == "ETHUSDC" if replacement else service._open_position() is None
        service.run_once()
        assert list(db.scalars(select(MomentumEngineTrade.action))) == actions


@pytest.mark.parametrize("last", [100, 101])
def test_nonnegative_closed_variation_and_live_candle_do_not_sell(last):
    with _make_session() as db:
        db.add_all(_momentum_row("BTCUSDC", price=100, momentum_score=30, stored_rank=1))
        add_candles(db, "15m", [100, last, 1], last_close=datetime.now(timezone.utc) + timedelta(minutes=10))
        db.commit()
        asset = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)._rankings()[0]
        assert asset["closed_change_15m"] >= 0
        assert not MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)._exit_required(asset)


def test_new_filters_do_not_affect_stock_etf():
    with _make_session() as db:
        service = MomentumEngineService(db, context=STOCK_ETF_CONTEXT)
        asset = {"structure_15m_status": "valid", "rsi_15m": 99, "rsi_1h": 99,
                 "rsi_recent_cross_down": True, "closed_change_15m": -2}
        assert service._entry_valid(asset)
        assert not service._exit_required(asset)


def test_unclosed_candle_cannot_create_rsi_cross_or_overbought_signal():
    with _make_session() as db:
        db.add_all(_momentum_row("BTCUSDC", price=100, momentum_score=30, stored_rank=1))
        # Closed RSI = 72.22. The future candle would push RSI above 80.
        add_candles(db, "15m", list(range(100, 115)) + [109, 200],
                    last_close=datetime.now(timezone.utc) + timedelta(minutes=10))
        db.commit()
        asset = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)._rankings()[0]
        assert asset["rsi_15m"] == pytest.approx(72.222222)
        assert asset["rsi_recent_cross_down"] is True


def test_cooldown_expires_exactly_one_hour_after_close(monkeypatch):
    from app.services import momentum_engine_service as module
    fixed_now = datetime(2026, 10, 17, 12, tzinfo=timezone.utc)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(module, "datetime", Clock)
    with _make_session() as db:
        db.add_all(_momentum_row("BTCUSDC", price=100, momentum_score=30, stored_rank=1))
        add_candles(db, "1h", list(range(100, 115)) + [109],
                    last_close=fixed_now - timedelta(hours=1))
        db.commit()
        assert MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)._rankings()[0]["rsi_recent_cross_down"] is False

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.services.momentum_service as momentum_module
from app.models.base import Base
from app.models.momentum_structure_current import MomentumStructureCurrent
from app.services.momentum_service import MomentumService


NOW = datetime(2026, 10, 19, 12, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def freeze_time(monkeypatch):
    class FrozenDatetimeMeta(type):
        def __instancecheck__(cls, instance):
            return isinstance(instance, datetime)

    class FrozenDatetime(datetime, metaclass=FrozenDatetimeMeta):
        @classmethod
        def now(cls, tz=None):
            return NOW.astimezone(tz) if tz else NOW.replace(tzinfo=None)

    monkeypatch.setattr(momentum_module, "datetime", FrozenDatetime)


def candles(count=10):
    start = NOW - timedelta(days=2)
    return [
        {
            "open_time": int((start + timedelta(minutes=15 * i)).timestamp() * 1000),
            "close_time": int((start + timedelta(minutes=15 * (i + 1))).timestamp() * 1000),
            "open": 100, "high": 101, "low": 99, "close": 100,
            "volume": 100, "ingested_at": NOW,
        }
        for i in range(count)
    ]


def breaking_candle(price, *, closed=False):
    return {
        "open_time": int((NOW - timedelta(minutes=5)).timestamp() * 1000),
        "close_time": int((NOW + timedelta(minutes=-1 if closed else 10)).timestamp() * 1000),
        "open": 100, "high": max(101, price + 1), "low": min(99, price - 1),
        "close": price, "volume": 100, "ingested_at": NOW,
    }


@pytest.mark.parametrize("price", [50, 150])
def test_open_breakout_cannot_change_structure_or_swing_levels(price):
    service = MomentumService.__new__(MomentumService)
    history = candles()
    baseline = service._structure_15m(history)
    assert baseline["structure_15m_status"] == "valid"
    assert service._structure_15m(history + [breaking_candle(price)]) == baseline


def test_open_bar_cannot_confirm_an_unfinished_swing():
    service = MomentumService.__new__(MomentumService)
    history = candles()
    # The low at index 8 has only one closed confirmation bar to its right.
    history[8].update(low=90, close=100)
    baseline = service._structure_15m(history)
    future = breaking_candle(100)
    assert baseline["last_swing_low_15m"] == 99
    assert service._structure_15m(history + [future]) == baseline


def test_minimum_history_counts_only_closed_bars():
    service = MomentumService.__new__(MomentumService)
    row = service._structure_15m(candles(7) + [breaking_candle(50)])
    assert row["structure_15m_status"] == "insufficient_data"
    assert row["last_swing_low_15m"] is None
    assert not row["bos_15m_bearish"]
    assert not row["mss_15m_bearish"]


@pytest.mark.parametrize("price, status, flag", [
    (50, "broken_bearish", "bos_15m_bearish"),
    (150, "valid_bullish", "bos_15m_bullish"),
])
def test_actual_closed_breakout_is_still_detected(price, status, flag):
    service = MomentumService.__new__(MomentumService)
    row = service._structure_15m(candles() + [breaking_candle(price, closed=True)])
    assert row["structure_15m_status"] == status
    assert row[flag]
    assert row["mss_15m_bearish"] is (price == 50)


def test_missing_close_timestamp_cannot_confirm_a_break():
    service = MomentumService.__new__(MomentumService)
    history = candles()
    unverified = breaking_candle(50)
    unverified.pop("close_time")
    assert service._structure_15m(history + [unverified]) == service._structure_15m(history)


def test_close_boundary_accepts_bar_only_when_closing_time_is_reached():
    service = MomentumService.__new__(MomentumService)
    history = candles()
    bar = breaking_candle(50)
    now_ms = int(NOW.timestamp() * 1000)
    bar["close_time"] = now_ms + 1
    assert service._structure_15m(history + [bar])["structure_15m_status"] == "valid"
    bar["close_time"] = now_ms
    assert service._structure_15m(history + [bar])["structure_15m_status"] == "broken_bearish"


@pytest.mark.parametrize("naive", [False, True])
def test_datetime_close_times_use_the_same_closure_filter(naive):
    service = MomentumService.__new__(MomentumService)
    history = candles()
    for bar in history:
        close_time = datetime.fromtimestamp(bar["close_time"] / 1000, tz=timezone.utc)
        bar["close_time"] = close_time.replace(tzinfo=None) if naive else close_time
    future = breaking_candle(50)
    future["close_time"] = NOW + timedelta(minutes=10)
    if naive:
        future["close_time"] = future["close_time"].replace(tzinfo=None)
    assert service._structure_15m(history + [future]) == service._structure_15m(history)
    future["close_time"] = NOW.replace(tzinfo=None) if naive else NOW
    assert service._structure_15m(history + [future])["structure_15m_status"] == "broken_bearish"


def test_recalculation_persists_only_confirmed_structure():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    history = candles()
    last_bar = breaking_candle(50)
    bundle = {"15m": history + [last_bar], "1h": [], "4h": []}
    with Session(engine) as db:
        service = MomentumService(db)
        service.market_data = SimpleNamespace(
            load_symbol_bundle=lambda symbol, limits: dict(bundle),
        )
        service.recalculate_and_store(symbols=["BTCUSD"])
        row = db.get(MomentumStructureCurrent, ("BTCUSD", "crypto"))
        assert row.structure_15m_status == "valid"
        assert not row.bos_15m_bearish
        assert row.structure_broken_at is None
        assert service.list_rankings()[0]["structure_15m_status"] == "valid"

        # The same break becomes actionable only after candle closure.
        last_bar["close_time"] = int((NOW - timedelta(seconds=1)).timestamp() * 1000)
        service.recalculate_and_store(symbols=["BTCUSD"])
        assert row.structure_15m_status == "broken_bearish"
        assert row.bos_15m_bearish
        assert row.mss_15m_bearish
        assert row.structure_broken_at is not None
        assert service.list_rankings()[0]["structure_15m_status"] == "broken_bearish"

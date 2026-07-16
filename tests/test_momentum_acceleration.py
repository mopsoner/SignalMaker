from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.services.momentum_service import MomentumService


class DummyMomentumService(MomentumService):
    def __init__(self, interval_payloads):
        self.interval_payloads = interval_payloads
        self.market_data = SimpleNamespace(load_symbol_bundle=lambda symbol, lookbacks: {})

    def _interval_momentum(self, candles):
        return self.interval_payloads.pop(0)

    def _structure_15m(self, candles):
        return {"structure_15m_status": "valid"}


BASE_CANDLE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


def payload(momentum, candle_time=BASE_CANDLE_TIME):
    return {
        "momentum": momentum,
        "change": None,
        "rsi": None,
        "ema_trend": "unknown",
        "updated_at": None,
        "candle_time": candle_time,
        "price": 1.0,
    }


def build_row(values, previous=None):
    service = DummyMomentumService([payload(v) for v in values])
    return service._build_symbol_row("BTCUSD", previous=previous)


def previous(**kwargs):
    defaults = {
        "momentum_15m": 0.0,
        "momentum_1h": 0.0,
        "momentum_4h": 0.0,
        "momentum_delta_15m": 0.0,
        "momentum_delta_1h": 0.0,
        "momentum_delta_4h": 0.0,
        "momentum_acceleration_15m": 0.0,
        "momentum_acceleration_1h": 0.0,
        "momentum_acceleration_4h": 0.0,
        "momentum_candle_time_15m": BASE_CANDLE_TIME - timedelta(minutes=15),
        "momentum_candle_time_1h": BASE_CANDLE_TIME - timedelta(hours=1),
        "momentum_candle_time_4h": BASE_CANDLE_TIME - timedelta(hours=4),
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_first_calculation_has_zero_deltas_and_acceleration():
    row = build_row([10.0, 20.0, 30.0])
    assert row["momentum_delta_15m"] == 0.0
    assert row["momentum_delta_1h"] == 0.0
    assert row["momentum_delta_4h"] == 0.0
    assert row["momentum_acceleration_15m"] == 0.0
    assert row["momentum_acceleration_1h"] == 0.0
    assert row["momentum_acceleration_4h"] == 0.0
    assert row["momentum_acceleration"] == 0.0


def test_constant_progression_has_zero_acceleration():
    row = build_row([0.0, 8.0, 0.0], previous(momentum_1h=5.0, momentum_delta_1h=3.0))
    assert row["momentum_delta_1h"] == 3.0
    assert row["momentum_acceleration_1h"] == 0.0


def test_positive_acceleration_and_deceleration():
    positive = build_row([0.0, 15.0, 0.0], previous(momentum_1h=10.0, momentum_delta_1h=2.0))
    decel = build_row([0.0, 11.0, 0.0], previous(momentum_1h=10.0, momentum_delta_1h=5.0))
    assert positive["momentum_acceleration_1h"] == 3.0
    assert decel["momentum_acceleration_1h"] == -4.0


def test_drop_acceleration_and_drop_slowdown():
    stronger_drop = build_row([-10.0, 0.0, 0.0], previous(momentum_15m=-5.0, momentum_delta_15m=-2.0))
    slowing_drop = build_row([0.0, 0.0, -11.0], previous(momentum_4h=-10.0, momentum_delta_4h=-5.0))
    assert stronger_drop["momentum_acceleration_15m"] == -3.0
    assert slowing_drop["momentum_acceleration_4h"] == 4.0


def test_global_acceleration_aggregation_and_score_adjustment():
    row = build_row(
        [13.0, 24.0, 39.0],
        previous(
            momentum_15m=10.0,
            momentum_1h=20.0,
            momentum_4h=30.0,
            momentum_delta_15m=2.0,
            momentum_delta_1h=2.0,
            momentum_delta_4h=2.0,
        ),
    )
    assert row["momentum_acceleration"] == (1.0 * 0.20 + 2.0 * 0.45 + 7.0 * 0.35)
    raw_score = 13.0 * 0.35 + 24.0 * 0.40 + 39.0 * 0.25
    assert row["momentum_score"] == round(raw_score + row["momentum_acceleration"] * 0.30, 4)


def test_acceleration_cap_only_limits_score_contribution():
    high = build_row([100.0, 100.0, 100.0], previous(momentum_15m=0.0, momentum_1h=0.0, momentum_4h=0.0))
    low = build_row([-100.0, -100.0, -100.0], previous(momentum_15m=0.0, momentum_1h=0.0, momentum_4h=0.0))
    assert high["momentum_acceleration"] == 100.0
    assert high["momentum_score"] == 109.0
    assert low["momentum_acceleration"] == -100.0
    assert low["momentum_score"] == -109.0


def test_missing_timeframe_is_ignored_for_acceleration_weighting():
    row = build_row([10.0, None, 20.0], previous(momentum_15m=5.0, momentum_4h=10.0))
    assert row["momentum_delta_1h"] == 0.0
    assert row["momentum_acceleration_1h"] == 0.0
    assert row["momentum_acceleration"] == 8.1818


def test_row_to_payload_returns_acceleration_fields():
    service = DummyMomentumService([])
    row = SimpleNamespace(
        rank=1,
        symbol="BTCUSD",
        price=1.0,
        momentum_15m=1.0,
        momentum_1h=2.0,
        momentum_4h=3.0,
        momentum_score=2.0,
        momentum_delta_15m=0.1,
        momentum_delta_1h=0.2,
        momentum_delta_4h=0.3,
        momentum_acceleration_15m=1.1,
        momentum_acceleration_1h=1.2,
        momentum_acceleration_4h=1.3,
        momentum_acceleration=1.2,
        momentum_candle_time_15m=BASE_CANDLE_TIME,
        momentum_candle_time_1h=BASE_CANDLE_TIME,
        momentum_candle_time_4h=BASE_CANDLE_TIME,
        classification="neutral_bull",
        rsi_15m=None,
        rsi_1h=None,
        rsi_4h=None,
        change_15m=None,
        change_1h=None,
        change_4h=None,
        ema_trend_15m="unknown",
        ema_trend_1h="unknown",
        ema_trend_4h="unknown",
        updated_at=None,
        data_quality="complete",
        calculated_at=None,
    )
    result = service._row_to_payload(row, 1)
    for key in (
        "momentum_delta_15m",
        "momentum_delta_1h",
        "momentum_delta_4h",
        "momentum_acceleration_15m",
        "momentum_acceleration_1h",
        "momentum_acceleration_4h",
        "momentum_acceleration",
        "momentum_candle_time_15m",
        "momentum_candle_time_1h",
        "momentum_candle_time_4h",
    ):
        assert key in result


def test_relaunch_without_new_candle_preserves_delta_acceleration_and_score():
    prev = previous(
        momentum_15m=10.0,
        momentum_delta_15m=4.0,
        momentum_acceleration_15m=2.0,
        momentum_candle_time_15m=BASE_CANDLE_TIME,
    )
    row = build_row([10.0, 0.0, 0.0], previous=prev)
    assert row["momentum_delta_15m"] == 4.0
    assert row["momentum_acceleration_15m"] == 2.0
    assert row["momentum_candle_time_15m"] == BASE_CANDLE_TIME
    assert "15m" not in row["updated_timeframes"]


def test_new_15m_candle_recalculates_only_15m():
    prev = previous(
        momentum_15m=10.0,
        momentum_1h=20.0,
        momentum_4h=30.0,
        momentum_delta_15m=4.0,
        momentum_delta_1h=5.0,
        momentum_delta_4h=6.0,
        momentum_acceleration_1h=1.5,
        momentum_acceleration_4h=2.5,
        momentum_candle_time_15m=BASE_CANDLE_TIME - timedelta(minutes=15),
        momentum_candle_time_1h=BASE_CANDLE_TIME,
        momentum_candle_time_4h=BASE_CANDLE_TIME,
    )
    row = build_row([16.0, 20.0, 30.0], previous=prev)
    assert row["momentum_delta_15m"] == 6.0
    assert row["momentum_acceleration_15m"] == 2.0
    assert row["momentum_delta_1h"] == 5.0
    assert row["momentum_acceleration_1h"] == 1.5
    assert row["momentum_delta_4h"] == 6.0
    assert row["momentum_acceleration_4h"] == 2.5
    assert row["updated_timeframes"] == ["15m"]


def test_interval_momentum_ignores_open_candle():
    service = MomentumService.__new__(MomentumService)
    now = datetime.now(timezone.utc)
    closed_open = int((now - timedelta(minutes=30)).timestamp() * 1000)
    closed_close = int((now - timedelta(minutes=15)).timestamp() * 1000)
    open_open = int((now - timedelta(minutes=10)).timestamp() * 1000)
    open_close = int((now + timedelta(minutes=5)).timestamp() * 1000)
    result = service._interval_momentum([
        {"open_time": closed_open - 900_000, "close_time": closed_open, "close": 100.0},
        {"open_time": closed_open, "close_time": closed_close, "close": 110.0},
        {"open_time": open_open, "close_time": open_close, "close": 150.0},
    ])
    assert result["price"] == 110.0
    assert result["candle_time"] == datetime.fromtimestamp(closed_open / 1000, tz=timezone.utc)

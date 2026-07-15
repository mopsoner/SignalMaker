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


def payload(momentum):
    return {"momentum": momentum, "change": None, "rsi": None, "ema_trend": "unknown", "updated_at": None, "price": 1.0}


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
    assert row["momentum_acceleration"] == (1.0 * 0.35 + 2.0 * 0.40 + 7.0 * 0.25)
    raw_score = 13.0 * 0.35 + 24.0 * 0.40 + 39.0 * 0.25
    assert row["momentum_score"] == round(raw_score + row["momentum_acceleration"] * 0.15, 4)


def test_acceleration_cap_only_limits_score_contribution():
    high = build_row([100.0, 100.0, 100.0], previous(momentum_15m=0.0, momentum_1h=0.0, momentum_4h=0.0))
    low = build_row([-100.0, -100.0, -100.0], previous(momentum_15m=0.0, momentum_1h=0.0, momentum_4h=0.0))
    assert high["momentum_acceleration"] == 100.0
    assert high["momentum_score"] == 103.0
    assert low["momentum_acceleration"] == -100.0
    assert low["momentum_score"] == -103.0


def test_missing_timeframe_is_ignored_for_acceleration_weighting():
    row = build_row([10.0, None, 20.0], previous(momentum_15m=5.0, momentum_4h=10.0))
    assert row["momentum_delta_1h"] == 0.0
    assert row["momentum_acceleration_1h"] == 0.0
    assert row["momentum_acceleration"] == 7.0833


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
    ):
        assert key in result

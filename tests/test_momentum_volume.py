from copy import deepcopy
from datetime import datetime, timedelta, timezone
from math import nan, inf

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models.base import Base
from app.models.momentum_current import MomentumCurrent
from app.models.momentum_structure_current import MomentumStructureCurrent
from app.schemas.momentum import MomentumRead
from app.services.momentum_service import MomentumService


def candles(*, ratio=1.0, direction=1, scale=100.0, count=32):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    out = []
    for index in range(count):
        close = 100 + direction * index * 0.1
        out.append({
            "open_time": int((start + timedelta(minutes=15 * index)).timestamp() * 1000),
            "close_time": int((start + timedelta(minutes=15 * (index + 1))).timestamp() * 1000),
            "open": close, "close": close, "high": close + 1, "low": close - 1,
            "volume": scale * (ratio if index >= count - 3 else 1),
        })
    return out


def bundle(**kwargs):
    return {tf: candles(**kwargs) for tf in MomentumService.INTERVALS}


def calculate(**kwargs):
    return MomentumService.calculate_bundle("BTCUSD", bundle(**kwargs))


@pytest.mark.parametrize("direction", [1, -1])
def test_excess_volume_changes_final_score_but_not_price_momentum(direction):
    normal = calculate(direction=direction)
    elevated = calculate(direction=direction, ratio=2)
    assert elevated["momentum_score"] == pytest.approx(normal["momentum_score"] + direction * 5)
    assert elevated["momentum_volume_score"] == direction * 5
    for tf in MomentumService.INTERVALS:
        assert elevated[f"relative_volume_{tf}"] == 2
        assert elevated[f"momentum_{tf}"] == normal[f"momentum_{tf}"]
        assert elevated[f"momentum_delta_{tf}"] == normal[f"momentum_delta_{tf}"]
    assert elevated["momentum_acceleration"] == normal["momentum_acceleration"]


@pytest.mark.parametrize("ratio", [0, 0.5, 1])
def test_normal_or_lower_volume_is_neutral(ratio):
    row = calculate(ratio=ratio)
    assert row["momentum_volume_score"] == 0
    assert row["relative_volume_15m"] == ratio


def test_flat_price_and_extreme_spikes_do_not_invent_unbounded_bullishness():
    assert calculate(ratio=1000, direction=0)["momentum_volume_score"] == 0
    assert calculate(ratio=1000)["momentum_volume_score"] == 10
    assert calculate(ratio=1000, direction=-1)["momentum_volume_score"] == -10


def test_volume_units_cancel_and_baseline_excludes_recent_bars():
    small = calculate(ratio=2, scale=1)
    large = calculate(ratio=2, scale=1_000_000)
    assert small["relative_volume_1h"] == large["relative_volume_1h"] == 2
    assert small["momentum_score"] == large["momentum_score"]


def test_only_last_twenty_baseline_bars_are_used():
    data = bundle(ratio=2)
    for series in data.values():
        for bar in series[:-23]:
            bar["volume"] = 1_000_000
    row = MomentumService.calculate_bundle("BTCUSD", data)
    assert row["relative_volume_15m"] == 2
    assert row["momentum_volume_score"] == 5


@pytest.mark.parametrize("bad", [None, -1, nan, inf, "invalid"])
@pytest.mark.parametrize("index", [-1, -10])
def test_invalid_volume_history_is_neutral(bad, index):
    data = bundle(ratio=2)
    for series in data.values():
        series[index]["volume"] = bad
    row = MomentumService.calculate_bundle("BTCUSD", data)
    assert row["relative_volume_15m"] is None
    assert row["momentum_volume_score"] == 0


def test_missing_volume_zero_baseline_and_short_history_are_unavailable():
    calculator = MomentumService.__new__(MomentumService)
    missing = candles()
    for bar in missing:
        bar.pop("volume")
    zero_baseline = candles(ratio=0)
    for bar in zero_baseline[:-3]:
        bar["volume"] = 0
    for series in (missing, zero_baseline, candles(count=22)):
        assert calculator._volume_signal(series) == (None, 0)
    assert calculator._volume_signal(candles(count=23, ratio=2)) == (2, 5)


def test_open_candle_does_not_change_volume_score():
    data = bundle(ratio=2)
    before = MomentumService.calculate_bundle("BTCUSD", data)
    for series in data.values():
        unclosed = deepcopy(series[-1])
        unclosed["open_time"] = int(datetime.now(timezone.utc).timestamp() * 1000)
        unclosed["close_time"] = unclosed["open_time"] + 900_000
        unclosed["volume"] = 1_000_000
        unclosed["close"] = 1
        series.append(unclosed)
    after = MomentumService.calculate_bundle("BTCUSD", data)
    assert after["momentum_score"] == before["momentum_score"]
    assert after["relative_volume_15m"] == before["relative_volume_15m"]


def test_missing_volume_retains_zero_weight_without_amplifying_other_timeframes():
    data = bundle(ratio=2)
    for tf in ("1h", "4h"):
        for bar in data[tf]:
            bar.pop("volume")
    row = MomentumService.calculate_bundle("BTCUSD", data)
    assert row["momentum_volume_score"] == 1.75  # 5 * 35%, not 5.
    data["1h"] = []
    data["4h"] = []
    partial = MomentumService.calculate_bundle("BTCUSD", data)
    assert partial["momentum_volume_score"] == 5
    empty = MomentumService.calculate_bundle("BTCUSD", {})
    assert empty["momentum_volume_score"] == 0


def test_volume_ranking_and_classification_use_new_final_score():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine, tables=[MomentumCurrent.__table__, MomentumStructureCurrent.__table__])
    with Session(engine) as db:
        service = MomentumService(db)
        for symbol, ratio in (("NORMAL", 1), ("STRONG", 3)):
            data = bundle(ratio=ratio)
            for series in data.values():
                for index, bar in enumerate(series):
                    bar.update(close=100 + (index % 2) * 0.2, open=100, high=101, low=99)
            row = MomentumService.calculate_bundle(symbol, data)
            # Test persistence and ranking together, with otherwise identical prices.
            service._upsert_current(row, calculated_at=datetime.now(timezone.utc))
        db.commit()
        rows = service.list_rankings()
        assert [r["symbol"] for r in rows] == ["STRONG", "NORMAL"]
        strong, normal = [MomentumRead.model_validate(r) for r in rows]
        assert strong.momentum_score == pytest.approx(normal.momentum_score + 10)
        assert strong.momentum_volume_score == 10
        assert strong.relative_volume_15m == 3
        assert normal.classification == "neutral_bull"
        assert strong.classification == "bull"


def test_repeated_calculation_on_same_closed_candles_preserves_score():
    data = bundle(ratio=2)
    first = MomentumService.calculate_bundle("BTCUSD", data)
    previous = MomentumCurrent(**{
        key: value for key, value in first.items()
        if key in MomentumCurrent.__table__.columns
    })
    second = MomentumService.calculate_bundle("BTCUSD", data, previous=previous)
    assert second["momentum_score"] == first["momentum_score"]
    assert second["momentum_volume_score"] == first["momentum_volume_score"]

@pytest.mark.parametrize("bad", [None, 0, -1, nan, inf])
@pytest.mark.parametrize("index", [-1, -4])
def test_invalid_price_does_not_create_volume_signal(bad, index):
    series = candles(ratio=2)
    series[index]["close"] = bad
    calculator = MomentumService.__new__(MomentumService)
    assert calculator._volume_signal(series) == (None, 0)


def test_overflowing_volume_history_is_neutral():
    series = candles()
    for bar in series:
        bar["volume"] = 1e308
    calculator = MomentumService.__new__(MomentumService)
    assert calculator._volume_signal(series) == (None, 0)

import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.core.candles import closed_candles
from app.core.config import settings
from app.models.base import Base
from app.models.market_candle import MarketCandle
from app.services.market_data_service import MarketDataService
from app.services.momentum_ai_service import MomentumAIService
from app.services.momentum_ai_tools import MomentumReadTools
from app.services.momentum_engine_service import MomentumEngineService
from app.services.momentum_service import MomentumService
from app.services.shared_market_analysis_service import SharedMarketAnalysisService
from app.services.signal_engine_service import SignalEngineService
from app.services.wyckoff_pipeline_service import WyckoffPipelineService
from app.strategy.legacy_engine import build_signal
from signalmaker.market_data.analysis_adapter import MarketAnalysisAdapter


NOW = datetime(2026, 10, 19, 12, tzinfo=timezone.utc)
NOW_MS = int(NOW.timestamp() * 1000)
STEPS = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}


def history(tf="15m", count=80):
    start = 1_700_000_000_000
    return [dict(open_time=start+i*STEPS[tf], close_time=start+(i+1)*STEPS[tf],
                 open=100+i/10, high=102+i/10, low=99+i/10, close=101+i/10,
                 volume=100+i) for i in range(count)]


def live_bar():
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return dict(open_time=now_ms-1000, close_time=now_ms+900_000,
                open=1, high=1_000_000, low=0.01, close=1_000_000, volume=1e12)


@pytest.mark.parametrize("close_time", [None, "invalid", float("nan"), float("inf"), NOW_MS+1])
def test_unverifiable_and_future_closes_are_rejected(close_time):
    assert closed_candles([{"close_time": close_time}], as_of=NOW) == []


@pytest.mark.parametrize("flag", [False, 0, "false", "False", "0", "open"])
def test_explicit_open_flag_vetoes_even_an_elapsed_close(flag):
    assert closed_candles([{"close_time": NOW_MS-1, "is_closed": flag}], as_of=NOW) == []


@pytest.mark.parametrize("close_time", [NOW_MS, NOW, NOW.replace(tzinfo=None), NOW.isoformat()])
def test_exact_close_boundary_and_normalized_timestamp_forms(close_time):
    bar = {"open_time": NOW_MS-1000, "close_time": close_time, "is_closed": True}
    assert closed_candles([bar], as_of=NOW) == [bar]


def test_true_flag_never_overrides_clock_or_inverted_times():
    assert closed_candles([{"close_time": NOW_MS+1, "is_closed": True}], as_of=NOW) == []
    assert closed_candles([{"open_time": NOW_MS, "close_time": NOW_MS-1}], as_of=NOW) == []


@pytest.fixture
def signal_config(monkeypatch):
    config = settings.signal_config()
    config["execution_interval"] = "15m"
    config["session_confirm_filter_enabled"] = False
    monkeypatch.setattr("app.services.signal_engine_service.get_runtime_signal_config", lambda: config)
    return config


@pytest.mark.parametrize("tf", STEPS)
def test_legacy_signal_and_hierarchy_ignore_open_candles_in_every_timeframe(tf, signal_config):
    bundle = {tf: history(tf) for tf in STEPS}
    baseline = SignalEngineService().compute_signal("TEST", bundle)
    baseline_legacy = build_signal("TEST", bundle["15m"], bundle["15m"], bundle["1h"], bundle["4h"], signal_config)
    contaminated = deepcopy(bundle)
    contaminated[tf].append(live_bar())
    assert SignalEngineService().compute_signal("TEST", contaminated) == baseline
    assert build_signal("TEST", contaminated["15m"], contaminated["15m"],
                        contaminated["1h"], contaminated["4h"], signal_config) == baseline_legacy


def test_public_wyckoff_pipeline_does_not_change_decision_on_live_spikes(signal_config):
    bundle = {tf: history(tf) for tf in STEPS}
    planner = SimpleNamespace(assess_signal=lambda signal: {
        "accepted": False, "reason": "test", "rr_ratio": None, "candidate": None,
    })
    service = WyckoffPipelineService(planner=planner)
    baseline = service.analyze(symbol="TEST", candles=bundle)
    contaminated = {tf: rows+[live_bar()] for tf, rows in bundle.items()}
    assert service.analyze(symbol="TEST", candles=contaminated) == baseline


@pytest.mark.parametrize("engine, minimum", [("momentum", 32), ("wyckoff_smc", 24)])
def test_shared_minimum_history_counts_only_closed_candles(engine, minimum):
    bundle = {tf: history(tf) for tf in STEPS}
    bundle["15m"] = history(count=minimum-1) + [live_bar()]
    result = SharedMarketAnalysisService().run(
        market_scope="crypto", asset_id="TEST", engine=engine, universe=None,
        asset_type=None, timeframes=tuple(STEPS), workflow_version="test",
        candles=bundle, symbol="TEST",
    )
    assert result["status"] == "insufficient_data"
    assert result["missing_timeframes"] == ["15m"]
    assert result["payload"]["candle_counts"]["15m"] == minimum-1


def test_all_open_bundle_cannot_create_a_wyckoff_signal(signal_config):
    bundle = {tf: [live_bar()] for tf in STEPS}
    with pytest.raises(ValueError, match="missing_required_timeframes"):
        WyckoffPipelineService().analyze(symbol="TEST", candles=bundle)
    with pytest.raises(KeyError, match="missing_15m_candles"):
        SignalEngineService().compute_signal("TEST", bundle)
    with pytest.raises(ValueError, match="missing_closed_candles"):
        build_signal("TEST", *([live_bar()] for _ in range(4)), signal_config)


def test_momentum_price_structure_and_acceleration_ignore_explicit_open_flags():
    bundle = {tf: history(tf) for tf in STEPS}
    baseline = MomentumService.calculate_bundle("TEST", bundle)
    for rows in bundle.values():
        rows.append({**rows[-1], "close": 1e6, "is_closed": False})
    assert MomentumService.calculate_bundle("TEST", bundle) == baseline


@pytest.mark.parametrize("flag", [False, "false", "0"])
def test_stock_normalization_honors_open_flags_even_with_past_timestamps(flag):
    adapter = MarketAnalysisAdapter(None)
    old = history()[0]
    assert adapter.normalize_engine_candles([{**old, "is_closed": flag}], symbol="TEST", timeframe="15m") == []


@pytest.fixture
def candle_db():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        yield db


def store_bar(db, name, *, close_time, interval="15m", price=100, open_time=None):
    bar = MarketCandle(candle_id=name, symbol="TEST", interval=interval,
                       open_time=close_time-900_000 if open_time is None else open_time,
                       close_time=close_time, open=price, high=price+1, low=price-1,
                       close=price, volume=100)
    db.add(bar)
    return bar


def test_sql_filters_before_limits_cursors_and_prices(candle_db):
    db = candle_db
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    store_bar(db, "closed-old", close_time=now_ms-1_800_000, price=100)
    store_bar(db, "closed-new", close_time=now_ms-900_000, price=110)
    for index in range(5):
        store_bar(db, f"live-{index}", close_time=now_ms+900_000+index, price=999)
    store_bar(db, "inverted", close_time=now_ms-1, open_time=now_ms, price=888)
    db.commit()
    data = MarketDataService(db)
    rows = data.load_symbol_bundle("TEST", {"15m": 2})["15m"]
    assert [r["close"] for r in rows] == [100, 110]
    assert data.list_candles(symbol="TEST", latest=True)[0].candle_id == "closed-new"
    assert data.list_candles(symbol="TEST", first=True)[0].candle_id == "closed-old"
    assert data.get_latest_close_times(["TEST"])["TEST"]["15m"] == now_ms-900_000
    assert MomentumEngineService(db)._latest_market_price("TEST") == (110, "market_candle:15m")


def test_momentum_price_falls_back_to_closed_hourly_when_15m_is_open(candle_db):
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    store_bar(candle_db, "live", close_time=now_ms+1_000_000, price=999)
    store_bar(candle_db, "hourly", interval="1h", close_time=now_ms-1000, price=100)
    candle_db.commit()
    assert MomentumEngineService(candle_db)._latest_market_price("TEST") == (100, "market_candle:1h")


def test_no_closed_market_data_produces_no_reference_price_or_cursor(candle_db):
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    store_bar(candle_db, "live", close_time=now_ms+900_000)
    candle_db.commit()
    service = MarketDataService(candle_db)
    assert service.load_symbol_bundle("TEST", {"15m": 2}) == {"15m": []}
    assert service.get_latest_close_times(["TEST"]) == {}
    assert MomentumEngineService(candle_db)._latest_market_price("TEST") is None


def test_ai_tools_only_see_verifiably_closed_bars(candle_db):
    from app.models.momentum_current import MomentumCurrent
    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    store_bar(candle_db, "closed", close_time=now_ms-1000)
    store_bar(candle_db, "live", close_time=now_ms+900_000)
    store_bar(candle_db, "inverted", close_time=now_ms-1, open_time=now_ms)
    candle_db.add(MomentumCurrent(symbol="TEST"))
    candle_db.commit()
    snapshot = MomentumAIService(candle_db).snapshot()
    assert len(snapshot["price_history"]["TEST"]["15m"]) == 1
    page = MomentumReadTools(candle_db, now.isoformat(), 7).read({
        "dataset": "candles", "symbol": "TEST", "interval": "15m",
    })
    assert [r["candle_id"] for r in page["items"]] == ["closed"]

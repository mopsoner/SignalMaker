from datetime import datetime, timedelta, timezone
import asyncio
from zoneinfo import ZoneInfo

from app.services.momentum_service import MomentumService
from signalmaker.market_data.analysis_adapter import MarketAnalysisAdapter


def _bar(opened, *, timeframe="1h", close=100, adjusted=None):
    duration = {"15m": 15, "1h": 60, "4h": 240}[timeframe]
    return {
        "timestamp": opened,
        "open": close - 1,
        "high": close + 1,
        "low": close - 2,
        "close": close,
        "adjusted_close": adjusted,
        "volume": 1_000,
        "close_time": opened + timedelta(minutes=duration),
    }


def test_normalization_converts_us_and_europe_dst_sessions_to_ordered_utc_without_filling_gaps():
    adapter = MarketAnalysisAdapter(None)
    paris = ZoneInfo("Europe/Paris")
    new_york = ZoneInfo("America/New_York")
    # The exchanges change offset on different Sundays in March.  UTC conversion
    # must use each exchange's rules, not a fixed dashboard offset.
    rows = [
        _bar(datetime(2026, 3, 9, 9, 0, tzinfo=paris)),
        _bar(datetime(2026, 3, 9, 9, 30, tzinfo=new_york)),
        _bar(datetime(2026, 3, 6, 15, 0, tzinfo=paris)),
    ]
    result = adapter.normalize_engine_candles(rows, symbol="TEST", timeframe="1h")

    assert [row["timestamp"] for row in result] == sorted(row["timestamp"] for row in result)
    assert all(row["is_closed"] and row["symbol"] == "TEST" and row["timeframe"] == "1h" for row in result)
    assert len(result) == 3  # weekend/overnight gaps are not synthesized
    assert result[0]["timestamp"] == int(datetime(2026, 3, 6, 14, tzinfo=timezone.utc).timestamp() * 1000)
    assert result[-1]["timestamp"] == int(datetime(2026, 3, 9, 13, 30, tzinfo=timezone.utc).timestamp() * 1000)


def test_normalization_deduplicates_and_uses_documented_adjusted_close_policy():
    adapter = MarketAnalysisAdapter(None)
    opened = datetime(2020, 8, 31, 13, 30, tzinfo=timezone.utc)
    result = adapter.normalize_engine_candles(
        [_bar(opened, close=500), _bar(opened, close=125, adjusted=124.5)],
        symbol="SPLIT", timeframe="1h",
    )
    assert len(result) == 1
    assert result[0]["close"] == 124.5
    assert set(result[0]) == {
        "timestamp", "open_time", "close_time", "open", "high", "low", "close",
        "volume", "symbol", "timeframe", "is_closed",
    }


def test_missing_multi_timeframe_history_returns_dashboard_diagnostics_without_daily_fallback():
    enough = datetime.now(timezone.utc) - timedelta(days=20)
    bundle = {
        "15m": [_bar(enough + timedelta(minutes=15 * i), timeframe="15m") for i in range(40)],
        "1h": [_bar(enough + timedelta(hours=i), timeframe="1h") for i in range(40)],
        "1d": [_bar(enough + timedelta(days=i), timeframe="4h") for i in range(40)],
    }

    class MissingFourHourAdapter(MarketAnalysisAdapter):
        async def load_stock_etf_candle_bundle(self, asset_id, timeframes=MomentumService.INTERVALS):
            return bundle

    result = asyncio.run(MissingFourHourAdapter(None).run_momentum_analysis("asset"))
    assert result["status"] == "insufficient_data"
    assert result["payload"]["missing_timeframes"] == ["4h"]
    assert result["payload"]["timeframe_mapping"]["4h"] is None
    assert result["payload"]["candle_counts"]["4h"] == 0
    assert "1d" not in result["payload"]["timeframe_mapping"]

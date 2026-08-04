from datetime import datetime, timedelta, timezone
import asyncio

from app.services.momentum_service import MomentumService
from signalmaker.market_data.analysis_adapter import MarketAnalysisAdapter


def _candles(count=40):
    start = datetime.now(timezone.utc) - timedelta(days=10)
    return [
        {
            "timestamp": start + timedelta(minutes=15 * index),
            "open_time": int((start + timedelta(minutes=15 * index)).timestamp() * 1000),
            "close_time": int((start + timedelta(minutes=15 * index + 14)).timestamp() * 1000),
            "open": 100 + index, "high": 101 + index, "low": 99 + index,
            "close": 100.5 + index, "volume": 1000,
        }
        for index in range(count)
    ]


def test_stock_adapter_uses_shared_momentum_ranking_calculation():
    bundle = {timeframe: _candles() for timeframe in MomentumService.INTERVALS}

    class SnapshotAdapter(MarketAnalysisAdapter):
        async def load_stock_etf_candle_bundle(self, asset_id, timeframes=MomentumService.INTERVALS):
            return bundle

    result = asyncio.run(SnapshotAdapter(None).run_momentum_analysis("asset-1"))
    expected = MomentumService.calculate_bundle("asset-1", bundle)

    assert result["score"] == expected["momentum_score"]
    assert result["payload"]["momentum_15m"] == expected["momentum_15m"]
    assert result["payload"]["momentum_1h"] == expected["momentum_1h"]
    assert result["payload"]["momentum_4h"] == expected["momentum_4h"]
    assert "ma50" not in result["payload"]


def test_daily_only_feeder_does_not_fabricate_intraday_momentum():
    class DailyOnlyAdapter(MarketAnalysisAdapter):
        async def load_stock_etf_candle_bundle(self, asset_id, timeframes=MomentumService.INTERVALS):
            return {"1d": _candles()}

    result = asyncio.run(DailyOnlyAdapter(None).run_momentum_analysis("asset-1", "1d"))
    assert result["signal"] == "NO_SIGNAL"
    assert result["payload"]["unavailable_timeframes"] == ["15m", "1h", "4h"]
    assert result["payload"]["timeframe_mapping"] == {"15m": None, "1h": None, "4h": None}

from unittest.mock import Mock

from app.services.market_data_service import INTERVAL_MS, MarketDataService
from app.services.pipeline_service import PipelineService


def _continuous_candles(interval: str, count: int) -> list[dict]:
    step = INTERVAL_MS[interval]
    return [
        {
            "open_time": index * step,
            "close_time": (index + 1) * step - 1,
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
            "volume": 10.0,
        }
        for index in range(count)
    ]


def _pipeline_service(bundles: list[dict[str, list[dict]]]) -> PipelineService:
    service = PipelineService.__new__(PipelineService)
    service.db = Mock()
    service.collector = Mock()
    service.collector.discover_symbols.return_value = ["BTCUSD"]
    service.live_runs = Mock()
    service.trade_candidates = Mock()
    service.asset_states = Mock()
    service.momentum = Mock()
    service.momentum.recalculate_and_store.return_value = {"momentum_rows_upserted": 0}
    service.market_data = Mock()
    service.market_data.get_latest_close_times.return_value = {}
    service.market_data.load_symbol_bundle.side_effect = bundles
    service.market_data.validate_candle_series.side_effect = (
        lambda interval, candles, min_count: MarketDataService.validate_candle_series(
            None, interval, candles, min_count=min_count
        )
    )
    service.wyckoff_pipeline = Mock()
    service.wyckoff_pipeline.analyze.return_value = (
        {"symbol": "BTCUSD", "pipeline": {}},
        {"candidate": {"symbol": "BTCUSD", "payload": {}, "notes": None}},
    )
    service._collect_interval_parallel = Mock(return_value=({}, []))
    service._order_symbols_for_analysis = Mock(return_value=["BTCUSD"])
    return service


def test_run_once_blocks_truncated_series_then_analyzes_continuous_series() -> None:
    intervals = ("15m", "1h", "4h")
    truncated_bundle = {interval: _continuous_candles(interval, 29) for interval in intervals}
    complete_bundle = {interval: _continuous_candles(interval, 30) for interval in intervals}
    service = _pipeline_service([truncated_bundle, complete_bundle])

    blocked_result = service.run_once()

    service.wyckoff_pipeline.analyze.assert_not_called()
    service.asset_states.upsert_from_signal.assert_not_called()
    service.trade_candidates.upsert_open_candidate.assert_not_called()
    assert blocked_result["symbols_scanned"] == 0
    assert blocked_result["asset_states_upserted"] == 0
    assert blocked_result["candidates_created"] == 0
    assert blocked_result["data_quality_counts"] == {
        "insufficient_count:29<30": 1,
        "1h:insufficient_count:29<30": 1,
        "4h:insufficient_count:29<30": 1,
    }

    resumed_result = service.run_once()

    service.wyckoff_pipeline.analyze.assert_called_once()
    service.asset_states.upsert_from_signal.assert_called_once()
    service.trade_candidates.upsert_open_candidate.assert_called_once()
    assert resumed_result["symbols_scanned"] == 1
    assert resumed_result["asset_states_upserted"] == 1
    assert resumed_result["candidates_created"] == 1


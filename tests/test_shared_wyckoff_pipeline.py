import asyncio
from copy import deepcopy

from app.services.runtime_settings import DEFAULT_SETTINGS
from app.services.wyckoff_pipeline_service import WyckoffPipelineService
from signalmaker.market_data.analysis_adapter import MarketAnalysisAdapter


DECISION_FIELDS = (
    "stage", "bias", "hierarchy_gate", "wyckoff_requirement", "one_hour_decision",
    "confirmation_model", "execution_trigger", "liquidity_context",
    "macro_liquidity_context", "entry_liquidity_context", "projected_target",
)


def _candles(step_ms: int) -> list[dict]:
    start = 1_700_000_000_000
    rows = []
    for index in range(80):
        price = 100 + index * 0.08
        rows.append({
            "timestamp": start + index * step_ms,
            "open_time": start + index * step_ms,
            "close_time": start + (index + 1) * step_ms,
            "open": price, "high": price + 1, "low": price - 1,
            "close": price + 0.15, "volume": 1_000 + index,
        })
    return rows


def test_crypto_and_stock_adapter_share_decision_pipeline(monkeypatch):
    monkeypatch.setattr("app.services.planner_service.load_runtime_settings", lambda: deepcopy(DEFAULT_SETTINGS))
    monkeypatch.setattr("app.services.signal_engine_service.get_runtime_signal_config", lambda: {
        "execution_interval": "15m", "rsi_period": 14, "swing_window": 8,
        "equal_level_tolerance_pct": 0.002, "session_timezone_offset_hours": 0,
        "session_confirm_filter_enabled": False,
        "entry_rsi": {"min": 45.0, "max": 65.0, "timeframe": "1h"},
        "signals": {"overbought": 70.0, "oversold": 30.0, "price_near_extreme_pct": 0.0025},
    })
    bundle = {"15m": _candles(900_000), "1h": _candles(3_600_000), "4h": _candles(14_400_000)}
    crypto, _ = WyckoffPipelineService().analyze(
        symbol="SAME", candles=bundle, market_context={"provider": "kraken"}
    )

    class SnapshotAdapter(MarketAnalysisAdapter):
        async def load_stock_etf_candle_bundle(self, asset_id, timeframes=("15m", "1h", "4h")):
            return {tf: bundle[tf] for tf in timeframes}

    asset = {
        "id": "asset-1", "provider_symbol": "SAME", "universe_name": "Europe ETF",
        "asset_type": "ETF", "currency": "EUR", "exchange_code": "SBF",
    }
    stock = asyncio.run(SnapshotAdapter(None).run_wyckoff_smc_analysis("asset-1", asset=asset))
    state = stock["state_payload"]

    assert all(field in state for field in DECISION_FIELDS)
    assert {field: state[field] for field in DECISION_FIELDS} == {
        field: crypto[field] for field in DECISION_FIELDS
    }
    assert state["market_context"] == {
        "asset_id": "asset-1", "provider_symbol": "SAME", "universe_name": "Europe ETF",
        "asset_type": "ETF", "currency": "EUR", "exchange_code": "SBF",
    }


def test_shared_pipeline_rejects_incomplete_timeframe_bundle():
    try:
        WyckoffPipelineService().analyze(symbol="SAME", candles={"15m": _candles(900_000)})
    except ValueError as exc:
        assert str(exc) == "missing_required_timeframes:1h,4h"
    else:
        raise AssertionError("incomplete bundles must not be silently transformed")

from app.services.signal_engine_service import SignalEngineService
from app.strategy import legacy_engine
from app.strategy.legacy_engine import _rsi_entry_profile


def test_entry_rsi_profile_prefers_45_65_band():
    profile = _rsi_entry_profile(50.0)

    assert profile["preferred"] is True
    assert profile["value"] == 50.0
    assert profile["timeframe"] == "1h"
    assert profile["source"] == "rsi_htf"
    assert profile["min"] == 45.0
    assert profile["max"] == 65.0
    assert profile["reason"] == "preferred_45_65_rsi_entry"


def test_entry_rsi_profile_flags_rsi_30_as_outside_preferred_entry_band():
    profile = _rsi_entry_profile(30.0)

    assert profile["preferred"] is False
    assert profile["value"] == 30.0
    assert profile["reason"] == "entry_rsi_outside_45_65_band"


def test_final_score_rewards_preferred_entry_rsi_and_penalizes_extreme_rsi():
    service = SignalEngineService()
    base_signal = {
        "legacy_score": 5.0,
        "macro_window_4h": {"valid": True, "side": "bull"},
        "refinement_context_1h": {"valid": True},
        "execution_trigger_5m": {},
        "zone_validity": {"valid": True},
        "wyckoff_requirement": {"status": "reclaimed_waiting_5m_confirm"},
        "pipeline": {},
        "bias": "bull_watch",
    }

    _, preferred_breakdown = service._compute_final_score(
        {**base_signal, "entry_rsi": {"value": 50.0, "preferred": True}}
    )
    _, extreme_breakdown = service._compute_final_score(
        {**base_signal, "entry_rsi": {"value": 30.0, "preferred": False}}
    )

    assert preferred_breakdown["entry_rsi"] == 1.25
    assert extreme_breakdown["entry_rsi"] == -0.25


def test_service_entry_rsi_profile_uses_htf_instead_of_main_rsi():
    profile = SignalEngineService()._entry_rsi_profile({"rsi_main": 50.0, "rsi_htf": 30.0})

    assert profile["value"] == 30.0
    assert profile["timeframe"] == "1h"
    assert profile["source"] == "rsi_htf"
    assert profile["preferred"] is False


def test_rsi_htf_50_can_create_wyckoff_buy_watch_without_oversold_rsi(monkeypatch):
    rsi_values = iter([40.0, 50.0, 50.0])
    monkeypatch.setattr(legacy_engine, "rsi", lambda values, period: next(rsi_values))
    candles = [
        {
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 100.0,
            "quote_volume": 1000.0,
            "number_of_trades": 100,
            "open_time": 1_700_000_000_000 + i * 900_000,
            "close_time": 1_700_000_000_000 + (i + 1) * 900_000,
        }
        for i in range(30)
    ]
    candles[-1] = {**candles[-1], "open": 100.0, "high": 100.5, "low": 99.0, "close": 99.02}
    cfg = {
        "rsi_period": 14,
        "swing_window": 8,
        "equal_level_tolerance_pct": 0.002,
        "session_timezone_offset_hours": 0,
        "execution_interval": "15m",
        "session_confirm_filter_enabled": False,
        "signals": {"overbought": 70.0, "oversold": 30.0, "price_near_extreme_pct": 0.0025},
    }

    signal = legacy_engine.build_signal("BTCUSDT", candles, candles, candles, candles, cfg)

    assert signal["rsi_main"] == 40.0
    assert signal["rsi_main_timeframe"] == "15m"
    assert signal["entry_rsi"]["value"] == 50.0
    assert signal["entry_rsi"]["timeframe"] == "1h"
    assert signal["entry_rsi"]["source"] == "rsi_htf"
    assert signal["entry_rsi"]["preferred"] is True
    assert signal["bias"] == "bull_watch"
    assert signal["pipeline"]["zone"] is True


def test_entry_rsi_profile_can_use_configured_macro_rsi_band():
    cfg = {"entry_rsi": {"min": 48.0, "max": 58.0, "timeframe": "4h"}}
    profile = _rsi_entry_profile(52.0, "4h", cfg)

    assert profile["preferred"] is True
    assert profile["timeframe"] == "4h"
    assert profile["source"] == "rsi_macro"
    assert profile["min"] == 48.0
    assert profile["max"] == 58.0


def test_service_entry_rsi_profile_uses_runtime_entry_rsi_config():
    cfg = {"entry_rsi": {"min": 48.0, "max": 58.0, "timeframe": "4h"}}
    profile = SignalEngineService()._entry_rsi_profile(
        {"rsi_main": 50.0, "rsi_htf": 30.0, "rsi_macro": 52.0},
        cfg,
    )

    assert profile["value"] == 52.0
    assert profile["timeframe"] == "4h"
    assert profile["source"] == "rsi_macro"
    assert profile["preferred"] is True

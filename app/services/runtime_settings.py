from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import settings as base_settings
from app.db.session import SessionLocal
from app.models.app_setting import AppSetting

DEFAULT_SETTINGS: dict[str, dict[str, Any]] = {
    "general": {
        "app_name": base_settings.app_name,
        "app_env": base_settings.app_env,
        "cors_origins": base_settings.cors_origins,
        "create_tables_on_boot": base_settings.create_tables_on_boot,
    },
    "binance": {
        "binance_rest_base": base_settings.binance_rest_base,
        "binance_quote_assets": base_settings.binance_quote_assets,
        "binance_symbol_status": base_settings.binance_symbol_status,
        "binance_max_symbols": base_settings.binance_max_symbols,
        "binance_lookback_1m": base_settings.binance_lookback_1m,
        "binance_lookback_5m": base_settings.binance_lookback_5m,
        "binance_lookback_1h": base_settings.binance_lookback_1h,
        "binance_lookback_4h": base_settings.binance_lookback_4h,
    },
    "strategy": {
        "session_timezone_offset_hours": base_settings.session_timezone_offset_hours,
        "signal_rsi_period": base_settings.signal_rsi_period,
        "signal_swing_window": base_settings.signal_swing_window,
        "signal_equal_level_tolerance_pct": base_settings.signal_equal_level_tolerance_pct,
        "signal_overbought": base_settings.signal_overbought,
        "signal_oversold": base_settings.signal_oversold,
        "signal_price_near_extreme_pct": base_settings.signal_price_near_extreme_pct,
        "signal_session_confirm_filter_enabled": base_settings.signal_session_confirm_filter_enabled,
        "planner_min_score": base_settings.planner_min_score,
        "planner_min_rr": base_settings.planner_min_rr,
    },
}


def load_runtime_settings(db: Session | None = None) -> dict[str, dict[str, Any]]:
    owns_session = db is None
    if db is None:
        db = SessionLocal()
    try:
        rows = db.execute(select(AppSetting)).scalars().all()
        payload = {section: values.copy() for section, values in DEFAULT_SETTINGS.items()}
        for row in rows:
            payload.setdefault(row.category, {})[row.key] = row.value
        return payload
    finally:
        if owns_session:
            db.close()


def get_runtime_signal_config(db: Session | None = None) -> dict[str, Any]:
    strategy = load_runtime_settings(db)["strategy"]
    return {
        "rsi_period": strategy["signal_rsi_period"],
        "swing_window": strategy["signal_swing_window"],
        "equal_level_tolerance_pct": strategy["signal_equal_level_tolerance_pct"],
        "session_timezone_offset_hours": strategy["session_timezone_offset_hours"],
        "session_confirm_filter_enabled": strategy["signal_session_confirm_filter_enabled"],
        "signals": {
            "overbought": strategy["signal_overbought"],
            "oversold": strategy["signal_oversold"],
            "price_near_extreme_pct": strategy["signal_price_near_extreme_pct"],
        },
    }

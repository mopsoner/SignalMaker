from __future__ import annotations

import os
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import settings as base_settings
from app.db.session import SessionLocal
from app.models.app_setting import AppSetting


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return default


def _entry_rsi_timeframe(value: Any) -> str:
    value = str(value or "1h").strip().lower()
    return value if value in {"1h", "4h"} else "1h"


DEFAULT_SETTINGS: dict[str, dict[str, Any]] = {
    "general": {
        "app_name": base_settings.app_name,
        "app_env": base_settings.app_env,
        "cors_origins": base_settings.cors_origins,
        "create_tables_on_boot": base_settings.create_tables_on_boot,
    },
    "binance": {
        "binance_rest_base": base_settings.binance_rest_base,
        "binance_collector_enabled": base_settings.binance_collector_enabled,
        "binance_quote_assets": base_settings.binance_quote_assets,
        "binance_symbol_status": base_settings.binance_symbol_status,
        "binance_max_symbols": base_settings.binance_max_symbols,
        "binance_min_quote_volume_24h": base_settings.binance_min_quote_volume_24h,
        "binance_min_trades_24h": base_settings.binance_min_trades_24h,
        "binance_excluded_base_assets": base_settings.binance_excluded_base_assets,
        "binance_collect_max_workers": base_settings.binance_collect_max_workers,
        "binance_incremental_fetch_enabled": base_settings.binance_incremental_fetch_enabled,
        "binance_incremental_min_1m": base_settings.binance_incremental_min_1m,
        "binance_incremental_min_5m": base_settings.binance_incremental_min_5m,
        "binance_incremental_min_15m": base_settings.binance_incremental_min_15m,
        "binance_incremental_min_1h": base_settings.binance_incremental_min_1h,
        "binance_incremental_min_4h": base_settings.binance_incremental_min_4h,
        "binance_lookback_1m": base_settings.binance_lookback_1m,
        "binance_lookback_5m": base_settings.binance_lookback_5m,
        "binance_lookback_15m": base_settings.binance_lookback_15m,
        "binance_lookback_1h": base_settings.binance_lookback_1h,
        "binance_lookback_4h": base_settings.binance_lookback_4h,
    },
    "strategy": {
        "session_timezone_offset_hours": base_settings.session_timezone_offset_hours,
        "signal_execution_interval": "15m",
        "signal_rsi_period": base_settings.signal_rsi_period,
        "signal_swing_window": base_settings.signal_swing_window,
        "signal_equal_level_tolerance_pct": base_settings.signal_equal_level_tolerance_pct,
        "signal_overbought": base_settings.signal_overbought,
        "signal_oversold": base_settings.signal_oversold,
        "signal_entry_rsi_min": base_settings.signal_entry_rsi_min,
        "signal_entry_rsi_max": base_settings.signal_entry_rsi_max,
        "signal_entry_rsi_timeframe": base_settings.signal_entry_rsi_timeframe,
        "signal_price_near_extreme_pct": base_settings.signal_price_near_extreme_pct,
        "signal_session_confirm_filter_enabled": base_settings.signal_session_confirm_filter_enabled,
        "planner_min_score": base_settings.planner_min_score,
        "planner_min_rr": base_settings.planner_min_rr,
    },
    "notifications": {
        "telegram_chat_id": base_settings.telegram_chat_id,
        "telegram_secret": base_settings.telegram_bot_token,
        "discord_url": base_settings.discord_webhook_url,
    },
    "bot": {
        "bot_pipeline_enabled": base_settings.bot_pipeline_enabled,
        "bot_executor_enabled": base_settings.bot_executor_enabled,
        "bot_scheduler_enabled": base_settings.bot_scheduler_enabled,
        "bot_momentum_engine_enabled": True,
        "bot_pipeline_interval_sec": base_settings.bot_pipeline_interval_sec,
        "bot_executor_interval_sec": base_settings.bot_executor_interval_sec,
        "bot_scheduler_interval_sec": base_settings.bot_scheduler_interval_sec,
        "bot_momentum_engine_interval_sec": 300,
        "bot_executor_limit": base_settings.bot_executor_limit,
        "bot_executor_quantity": base_settings.bot_executor_quantity,
    },
    "momentum": {
        "momentum_engine_enabled": True,
        "momentum_engine_interval_sec": 300,
        "momentum_engine_cadence_hours": 4,
        "momentum_engine_starting_capital": 1000.0,
        "momentum_engine_min_score": 0.0,
    },

    "ibkr": {
        "ibkr_enabled": os.getenv("IBKR_ENABLED", "false").lower() == "true",
        "ibkr_host": os.getenv("IBKR_HOST", "127.0.0.1"),
        "ibkr_port": int(os.getenv("IBKR_PORT", "4002")),
        "ibkr_client_id": int(os.getenv("IBKR_CLIENT_ID", "21")),
        "ibkr_historical_max_concurrent": int(os.getenv("IBKR_HISTORICAL_MAX_CONCURRENT", "2")),
        "ibkr_historical_sleep_seconds": int(os.getenv("IBKR_HISTORICAL_SLEEP_SECONDS", "12")),
        "ibkr_historical_duration": os.getenv("IBKR_HISTORICAL_DURATION", "2 Y"),
        "ibkr_historical_bar_size": os.getenv("IBKR_HISTORICAL_BAR_SIZE", "1 day"),
        "ibkr_historical_use_rth": os.getenv("IBKR_HISTORICAL_USE_RTH", "true").lower() == "true",
        "ibkr_historical_what_to_show": os.getenv("IBKR_HISTORICAL_WHAT_TO_SHOW", "TRADES"),
        "ibkr_momentum_lookback_days": int(os.getenv("IBKR_MOMENTUM_LOOKBACK_DAYS", "180")),
    },
    "live": {
        "live_trading_enabled": base_settings.live_trading_enabled,
        "binance_use_testnet": base_settings.binance_use_testnet,
        "binance_testnet_rest_base": base_settings.binance_testnet_rest_base,
        "live_spot_allow_shorts": base_settings.live_spot_allow_shorts,
        "live_max_open_positions": base_settings.live_max_open_positions,
        "live_max_notional_per_trade": base_settings.live_max_notional_per_trade,
        "live_require_tp_sl": base_settings.live_require_tp_sl,
        "live_reconcile_enabled": base_settings.live_reconcile_enabled,
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
        strategy = payload.setdefault("strategy", {})
        strategy["signal_execution_interval"] = "15m"
        strategy["signal_entry_rsi_timeframe"] = _entry_rsi_timeframe(strategy.get("signal_entry_rsi_timeframe"))
        payload.setdefault("binance", {})["binance_collector_enabled"] = _as_bool(
            payload.get("binance", {}).get("binance_collector_enabled", True),
            default=True,
        )
        payload.setdefault("bot", {})["bot_momentum_engine_enabled"] = _as_bool(
            payload.get("bot", {}).get("bot_momentum_engine_enabled", True),
            default=True,
        )
        momentum = payload.setdefault("momentum", {})
        momentum["momentum_engine_enabled"] = _as_bool(
            momentum.get("momentum_engine_enabled", True),
            default=True,
        )

        ibkr = payload.setdefault("ibkr", {})
        ibkr["ibkr_enabled"] = _as_bool(ibkr.get("ibkr_enabled", False), default=False)
        ibkr["ibkr_historical_use_rth"] = _as_bool(ibkr.get("ibkr_historical_use_rth", True), default=True)
        return payload
    finally:
        if owns_session:
            db.close()


def persist_runtime_settings(db: Session, payload: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    strategy = payload.get("strategy")
    if isinstance(strategy, dict):
        strategy["signal_execution_interval"] = "15m"
        if "signal_entry_rsi_timeframe" in strategy:
            strategy["signal_entry_rsi_timeframe"] = _entry_rsi_timeframe(strategy["signal_entry_rsi_timeframe"])

    binance = payload.get("binance")
    if isinstance(binance, dict) and "binance_collector_enabled" in binance:
        binance["binance_collector_enabled"] = _as_bool(binance["binance_collector_enabled"], default=True)

    bot = payload.get("bot")
    if isinstance(bot, dict) and "bot_momentum_engine_enabled" in bot:
        bot["bot_momentum_engine_enabled"] = _as_bool(bot["bot_momentum_engine_enabled"], default=True)

    momentum = payload.get("momentum")
    if isinstance(momentum, dict) and "momentum_engine_enabled" in momentum:
        momentum["momentum_engine_enabled"] = _as_bool(momentum["momentum_engine_enabled"], default=True)


    ibkr = payload.get("ibkr")
    if isinstance(ibkr, dict):
        if "ibkr_enabled" in ibkr:
            ibkr["ibkr_enabled"] = _as_bool(ibkr["ibkr_enabled"], default=False)
        if "ibkr_historical_use_rth" in ibkr:
            ibkr["ibkr_historical_use_rth"] = _as_bool(ibkr["ibkr_historical_use_rth"], default=True)

    for category, values in payload.items():
        if not isinstance(values, dict):
            continue
        for key, value in values.items():
            row = db.execute(
                select(AppSetting).where(AppSetting.category == category, AppSetting.key == key)
            ).scalar_one_or_none()
            if row is None:
                db.add(AppSetting(category=category, key=key, value=value))
            else:
                row.value = value
    db.commit()
    return load_runtime_settings(db)


def get_runtime_signal_config(db: Session | None = None) -> dict[str, Any]:
    strategy = load_runtime_settings(db)["strategy"]
    return {
        "execution_interval": "15m",
        "rsi_period": strategy["signal_rsi_period"],
        "swing_window": strategy["signal_swing_window"],
        "equal_level_tolerance_pct": strategy["signal_equal_level_tolerance_pct"],
        "session_timezone_offset_hours": strategy["session_timezone_offset_hours"],
        "session_confirm_filter_enabled": strategy["signal_session_confirm_filter_enabled"],
        "entry_rsi": {
            "min": strategy.get("signal_entry_rsi_min", 45.0),
            "max": strategy.get("signal_entry_rsi_max", 55.0),
            "timeframe": strategy.get("signal_entry_rsi_timeframe", "1h"),
        },
        "signals": {
            "overbought": strategy["signal_overbought"],
            "oversold": strategy["signal_oversold"],
            "price_near_extreme_pct": strategy["signal_price_near_extreme_pct"],
        },
    }


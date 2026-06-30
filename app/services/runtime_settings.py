from __future__ import annotations

import os
from typing import Any

from sqlalchemy import select, delete
from sqlalchemy.orm import Session

from app.core.config import settings as base_settings
from app.db.session import SessionLocal
from app.models.app_setting import AppSetting

DEFAULT_SETTINGS: dict[str, dict[str, Any]] = {
    "general": {
        "admin_token": base_settings.admin_token,
        "app_name": base_settings.app_name,
        "app_env": base_settings.app_env,
        "cors_origins": base_settings.cors_origins,
        "create_tables_on_boot": base_settings.create_tables_on_boot,
    },
    "executor": {
        "execution_exchange": os.getenv("EXECUTION_EXCHANGE", "binance"),
        "quote_assets": base_settings.binance_quote_assets,
    },
    "binance": {
        "binance_exchange_name": "binance",
        "binance_rest_base": base_settings.binance_rest_base,
        "binance_api_key": base_settings.binance_api_key,
        "binance_secret_key": base_settings.binance_secret_key,
    },
    "kraken": {
        "kraken_exchange_name": "kraken",
        "kraken_base_url": os.getenv("KRAKEN_BASE_URL", "https://api.kraken.com"),
        "kraken_api_key": os.getenv("KRAKEN_API_KEY", ""),
        "kraken_secret_key": os.getenv("KRAKEN_SECRET_KEY", ""),
    },
    "market_data": {
        "binance_collector_enabled": getattr(base_settings, "binance_collector_enabled", True),
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
        "bot_pipeline_interval_sec": base_settings.bot_pipeline_interval_sec,
        "bot_executor_interval_sec": base_settings.bot_executor_interval_sec,
        "bot_scheduler_interval_sec": base_settings.bot_scheduler_interval_sec,
        "bot_executor_limit": base_settings.bot_executor_limit,
        "bot_executor_quantity": base_settings.bot_executor_quantity,
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
    "momentum": {
        "signalmaker_base_url": base_settings.signalmaker_base_url,
        "momentum_candidates_sync_enabled": base_settings.momentum_candidates_sync_enabled,
        "momentum_candidates_limit": base_settings.momentum_candidates_limit,
        "momentum_candidates_min_score": base_settings.momentum_candidates_min_score,
        "momentum_candidates_min_rr": base_settings.momentum_candidates_min_rr,
        "momentum_candidates_require_wyckoff_context": base_settings.momentum_candidates_require_wyckoff_context,
        "momentum_candidates_http_timeout_sec": base_settings.momentum_candidates_http_timeout_sec,
        "momentum_candidates_source_path": base_settings.momentum_candidates_source_path,
        "momentum_candidates_target_pct": base_settings.momentum_candidates_target_pct,
    },
}

ADMIN_FIELD_ALIASES: dict[str, dict[str, str]] = {
    "general": {"ADMIN_TOKEN": "admin_token"},
    "executor": {
        "EXECUTION_EXCHANGE": "execution_exchange",
        "QUOTE_ASSETS": "quote_assets",
    },
    "binance": {
        "BINANCE_BASE_URL": "binance_rest_base",
        "BINANCE_API_KEY": "binance_api_key",
        "BINANCE_SECRET_KEY": "binance_secret_key",
        "BINANCE_USE_TESTNET": "binance_use_testnet",
    },
    "kraken": {
        "KRAKEN_BASE_URL": "kraken_base_url",
        "KRAKEN_API_KEY": "kraken_api_key",
        "KRAKEN_SECRET_KEY": "kraken_secret_key",
    },
    "notifications": {
        "TELEGRAM_BOT_TOKEN": "telegram_secret",
        "TELEGRAM_CHAT_ID": "telegram_chat_id",
        "DISCORD_WEBHOOK_URL": "discord_url",
    },
    "bot": {
        "BOT_PIPELINE_ENABLED": "bot_pipeline_enabled",
        "BOT_EXECUTOR_ENABLED": "bot_executor_enabled",
        "BOT_SCHEDULER_ENABLED": "bot_scheduler_enabled",
        "BOT_PIPELINE_INTERVAL_SEC": "bot_pipeline_interval_sec",
        "BOT_EXECUTOR_INTERVAL_SEC": "bot_executor_interval_sec",
    },
    "live": {
        "LIVE_TRADING_ENABLED": "live_trading_enabled",
        "LIVE_MAX_OPEN_POSITIONS": "live_max_open_positions",
        "LIVE_MAX_NOTIONAL_PER_TRADE": "live_max_notional_per_trade",
    },
    "momentum": {
        "MOMENTUM_CANDIDATES_SYNC_ENABLED": "momentum_candidates_sync_enabled",
        "MOMENTUM_CANDIDATES_LIMIT": "momentum_candidates_limit",
        "MOMENTUM_CANDIDATES_MIN_SCORE": "momentum_candidates_min_score",
    },
    "admin/security": {"ADMIN_TOKEN": "admin_token"},
}



LEGACY_ADMIN_SETTING_ALIASES: dict[tuple[str, str], tuple[str, str]] = {
    (section, display_key): (("general" if section == "admin/security" else section), runtime_key)
    for section, aliases in ADMIN_FIELD_ALIASES.items()
    for display_key, runtime_key in aliases.items()
}
LEGACY_ADMIN_SETTING_ALIASES[("kraken", "EXECUTION_EXCHANGE")] = ("executor", "execution_exchange")


def _has_value(value: Any) -> bool:
    return value is not None and str(value) != ""


def _migrate_legacy_admin_setting_rows(db: Session, rows: list[AppSetting]) -> list[AppSetting]:
    """Move legacy display-key rows to canonical rows without letting aliases win.

    Canonical lowercase rows are the source of truth.  Legacy uppercase rows are
    copied only when the canonical destination is empty/missing, then deleted so
    future loads cannot be ambiguous.
    """
    by_location = {(row.category, row.key): row for row in rows}
    changed = False
    for source, target in LEGACY_ADMIN_SETTING_ALIASES.items():
        alias_row = by_location.get(source)
        if alias_row is None:
            continue
        target_row = by_location.get(target)
        if target_row is None and _has_value(alias_row.value):
            target_row = AppSetting(category=target[0], key=target[1], value=alias_row.value)
            db.add(target_row)
            by_location[target] = target_row
            rows.append(target_row)
            changed = True
        elif target_row is not None and not _has_value(target_row.value) and _has_value(alias_row.value):
            target_row.value = alias_row.value
            changed = True
        db.delete(alias_row)
        if alias_row in rows:
            rows.remove(alias_row)
        changed = True
    if changed:
        db.commit()
        rows = db.execute(select(AppSetting)).scalars().all()
    return rows


def _apply_legacy_admin_setting_locations(payload: dict[str, dict[str, Any]]) -> None:
    """Keep old persisted admin rows working after splitting executor/market data settings."""
    binance = payload.setdefault("binance", {})
    executor = payload.setdefault("executor", {})
    kraken = payload.setdefault("kraken", {})
    market_data = payload.setdefault("market_data", {})

    for legacy_key in ("execution_exchange", "EXECUTION_EXCHANGE"):
        if legacy_key in kraken:
            executor["execution_exchange"] = kraken.pop(legacy_key)
    for legacy_key in ("binance_quote_assets", "BINANCE_QUOTE_ASSETS"):
        if legacy_key in binance:
            executor["quote_assets"] = binance.get(legacy_key)
    market_data_keys = {key for key in DEFAULT_SETTINGS["market_data"] if key != "binance_quote_assets"}
    market_data_keys.add("binance_quote_assets")
    for key in list(binance.keys()):
        if key in market_data_keys:
            market_data[key] = binance.pop(key)
    market_data.setdefault("binance_quote_assets", executor.get("quote_assets", base_settings.binance_quote_assets))

def _canonical_admin_field(section: str, key: str) -> tuple[str, str]:
    aliases = ADMIN_FIELD_ALIASES.get(section, {})
    if key in aliases:
        target_section = "general" if section == "admin/security" else section
        return target_section, aliases[key]
    return section, key


def _with_admin_aliases(payload: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    admin_payload = {section: values.copy() for section, values in payload.items()}
    for section in ("general", "executor", "binance", "kraken", "market_data", "strategy", "notifications", "bot", "live", "momentum", "admin/security"):
        admin_payload.setdefault(section, {})
    for section, aliases in ADMIN_FIELD_ALIASES.items():
        for display_key, runtime_key in aliases.items():
            source_section = "general" if section == "admin/security" else section
            if runtime_key in payload.get(source_section, {}):
                admin_payload[section][display_key] = payload[source_section][runtime_key]
    return admin_payload


def _normalize_admin_payload(payload: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    normalized = {section: values.copy() for section, values in payload.items() if isinstance(values, dict)}
    for section, aliases in ADMIN_FIELD_ALIASES.items():
        values = payload.get(section, {})
        if not isinstance(values, dict):
            continue
        target_section = "general" if section == "admin/security" else section
        normalized.setdefault(target_section, {})
        for display_key, runtime_key in aliases.items():
            if display_key in values:
                normalized[target_section][runtime_key] = values[display_key]
                if target_section == section:
                    normalized[target_section].pop(display_key, None)
    return normalized


ADMIN_EDITABLE_FIELDS: dict[str, tuple[str, ...]] = {
    "general": ("app_name", "app_env", "cors_origins", "create_tables_on_boot"),
    "executor": ("execution_exchange", "quote_assets"),
    "binance": ("binance_exchange_name", "binance_rest_base", "binance_api_key", "binance_secret_key"),
    "kraken": ("kraken_exchange_name", "kraken_base_url", "kraken_api_key", "kraken_secret_key"),
    "notifications": ("telegram_chat_id", "telegram_secret", "discord_url"),
    "bot": (
        "bot_pipeline_enabled",
        "bot_executor_enabled",
        "bot_scheduler_enabled",
        "bot_pipeline_interval_sec",
        "bot_executor_interval_sec",
        "bot_scheduler_interval_sec",
        "bot_executor_limit",
        "bot_executor_quantity",
    ),
    "live": (
        "live_trading_enabled",
        "live_spot_allow_shorts",
        "live_max_open_positions",
        "live_max_notional_per_trade",
        "live_require_tp_sl",
        "live_reconcile_enabled",
    ),
    "momentum": (
        "signalmaker_base_url",
        "momentum_candidates_sync_enabled",
        "momentum_candidates_limit",
        "momentum_candidates_min_score",
        "momentum_candidates_min_rr",
        "momentum_candidates_require_wyckoff_context",
        "momentum_candidates_http_timeout_sec",
        "momentum_candidates_source_path",
        "momentum_candidates_target_pct",
    ),
}


def _admin_editable_payload(payload: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Return only fields intentionally edited from the lightweight admin UI."""
    admin_payload: dict[str, dict[str, Any]] = {}
    for section, keys in ADMIN_EDITABLE_FIELDS.items():
        values = payload.get(section, {}) if isinstance(payload.get(section), dict) else {}
        defaults = DEFAULT_SETTINGS.get(section, {})
        section_payload: dict[str, Any] = {}
        for key in keys:
            value = values.get(key, defaults.get(key, ""))
            if value is None:
                value = ""
            section_payload[key] = value
        admin_payload[section] = section_payload
    return admin_payload


def load_admin_settings(db: Session | None = None) -> dict[str, dict[str, Any]]:
    return _admin_editable_payload(load_runtime_settings(db))


def load_runtime_settings(db: Session | None = None) -> dict[str, dict[str, Any]]:
    owns_session = db is None
    if db is None:
        db = SessionLocal()
    try:
        rows = db.execute(select(AppSetting)).scalars().all()
        if all(hasattr(db, attr) for attr in ("add", "delete", "commit")):
            rows = _migrate_legacy_admin_setting_rows(db, rows)
        payload = {section: values.copy() for section, values in DEFAULT_SETTINGS.items()}
        seen_canonical: set[tuple[str, str]] = set()
        for row in rows:
            original = (row.category, row.key)
            category, key = _canonical_admin_field(row.category, row.key)
            target = (category, key)
            is_alias = original != target or original in LEGACY_ADMIN_SETTING_ALIASES
            if is_alias and target in seen_canonical and _has_value(payload.get(category, {}).get(key)):
                continue
            payload.setdefault(category, {})[key] = row.value
            if not is_alias:
                seen_canonical.add(target)
        payload.setdefault("strategy", {})["signal_execution_interval"] = "15m"
        payload.setdefault("market_data", {})["binance_collector_enabled"] = bool(payload.get("market_data", {}).get("binance_collector_enabled", True))
        _apply_legacy_admin_setting_locations(payload)
        payload.setdefault("momentum", {})["momentum_candidates_sync_enabled"] = bool(payload.get("momentum", {}).get("momentum_candidates_sync_enabled", False))
        payload.setdefault("momentum", {})["momentum_candidates_require_wyckoff_context"] = bool(payload.get("momentum", {}).get("momentum_candidates_require_wyckoff_context", True))
        return payload
    finally:
        if owns_session:
            db.close()


def persist_runtime_settings(db: Session, payload: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    payload = _normalize_admin_payload(payload)
    for source in LEGACY_ADMIN_SETTING_ALIASES:
        db.execute(delete(AppSetting).where(AppSetting.category == source[0], AppSetting.key == source[1]))
    strategy = payload.get("strategy")
    if isinstance(strategy, dict):
        strategy["signal_execution_interval"] = "15m"

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
        "signals": {
            "overbought": strategy["signal_overbought"],
            "oversold": strategy["signal_oversold"],
            "price_near_extreme_pct": strategy["signal_price_near_extreme_pct"],
        },
    }


def get_runtime_momentum_config(db: Session | None = None) -> dict[str, Any]:
    return load_runtime_settings(db)["momentum"]

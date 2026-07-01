from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select, delete
from sqlalchemy.orm import Session

from app.core.config import settings as base_settings
from app.db.session import SessionLocal
from app.models.app_setting import AppSetting


# Canonical runtime source-of-truth policy:
# - app_settings rows loaded by this module are authoritative for API/admin UI runtime settings.
# - .env/BaseSettings are bootstrap defaults only and may seed missing app_settings rows.
# - Raspberry local SQLite settings are legacy fallback input only during migration.
BOOTSTRAP_ENV_ALIASES: dict[str, tuple[str, str]] = {
    "ADMIN_TOKEN": ("general", "admin_token"),
    "APP_NAME": ("general", "app_name"),
    "APP_ENV": ("general", "app_env"),
    "CORS_ORIGINS": ("general", "cors_origins"),
    "CREATE_TABLES_ON_BOOT": ("general", "create_tables_on_boot"),
    "EXECUTION_EXCHANGE": ("executor", "execution_exchange"),
    "QUOTE_ASSETS": ("executor", "quote_assets"),
    "KRAKEN_QUOTE_ASSETS": ("executor", "quote_assets"),
    "KRAKEN_BASE_URL": ("kraken", "kraken_base_url"),
    "KRAKEN_REST_BASE": ("kraken", "kraken_base_url"),
    "KRAKEN_API_KEY": ("kraken", "kraken_api_key"),
    "KRAKEN_SECRET_KEY": ("kraken", "kraken_secret_key"),
    "TELEGRAM_BOT_TOKEN": ("notifications", "telegram_secret"),
    "TELEGRAM_CHAT_ID": ("notifications", "telegram_chat_id"),
    "DISCORD_WEBHOOK_URL": ("notifications", "discord_url"),
    "LIVE_TRADING_ENABLED": ("live", "live_trading_enabled"),
    "KRAKEN_USE_TESTNET": ("live", "kraken_use_testnet"),
    "LIVE_MAX_OPEN_POSITIONS": ("live", "live_max_open_positions"),
    "LIVE_MAX_NOTIONAL_PER_TRADE": ("live", "live_max_notional_per_trade"),
    "SIGNALMAKER_BASE_URL": ("momentum", "signalmaker_base_url"),
    "MOMENTUM_CANDIDATES_SYNC_ENABLED": ("momentum", "momentum_candidates_sync_enabled"),
    "MOMENTUM_CANDIDATES_LIMIT": ("momentum", "momentum_candidates_limit"),
    "MOMENTUM_CANDIDATES_MIN_SCORE": ("momentum", "momentum_candidates_min_score"),
}

LEGACY_RASPBERRY_SETTING_ALIASES: dict[str, tuple[str, str]] = {
    **BOOTSTRAP_ENV_ALIASES,
    "ALLOWED_SYMBOLS": ("executor", "quote_assets"),
    "EXECUTION_QUOTE_ASSET": ("executor", "quote_assets"),
    "CANDLE_FEED_QUOTES": ("executor", "quote_assets"),
    "CANDLE_FEED_QUOTE_ASSETS": ("executor", "quote_assets"),
}

DEFAULT_SETTINGS: dict[str, dict[str, Any]] = {
    "general": {
        "admin_token": base_settings.admin_token,
        "app_name": base_settings.app_name,
        "app_env": base_settings.app_env,
        "cors_origins": base_settings.cors_origins,
        "create_tables_on_boot": base_settings.create_tables_on_boot,
    },
    "executor": {
        "execution_exchange": os.getenv("EXECUTION_EXCHANGE", "kraken"),
        "quote_assets": base_settings.kraken_quote_assets,
    },
    "kraken": {
        "kraken_exchange_name": "kraken",
        "kraken_base_url": base_settings.kraken_base_url,
        "kraken_api_key": base_settings.kraken_api_key,
        "kraken_secret_key": base_settings.kraken_secret_key,
    },
    "market_data": {
        "kraken_collector_enabled": getattr(base_settings, "kraken_collector_enabled", True),
        "kraken_quote_assets": base_settings.kraken_quote_assets,
        "kraken_symbol_status": base_settings.kraken_symbol_status,
        "kraken_max_symbols": base_settings.kraken_max_symbols,
        "kraken_min_quote_volume_24h": base_settings.kraken_min_quote_volume_24h,
        "kraken_min_trades_24h": base_settings.kraken_min_trades_24h,
        "kraken_excluded_base_assets": base_settings.kraken_excluded_base_assets,
        "kraken_collect_max_workers": base_settings.kraken_collect_max_workers,
        "kraken_incremental_fetch_enabled": base_settings.kraken_incremental_fetch_enabled,
        "kraken_incremental_min_1m": base_settings.kraken_incremental_min_1m,
        "kraken_incremental_min_5m": base_settings.kraken_incremental_min_5m,
        "kraken_incremental_min_15m": base_settings.kraken_incremental_min_15m,
        "kraken_incremental_min_1h": base_settings.kraken_incremental_min_1h,
        "kraken_incremental_min_4h": base_settings.kraken_incremental_min_4h,
        "kraken_lookback_1m": base_settings.kraken_lookback_1m,
        "kraken_lookback_5m": base_settings.kraken_lookback_5m,
        "kraken_lookback_15m": base_settings.kraken_lookback_15m,
        "kraken_lookback_1h": base_settings.kraken_lookback_1h,
        "kraken_lookback_4h": base_settings.kraken_lookback_4h,
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
        "kraken_use_testnet": base_settings.kraken_use_testnet,
        "kraken_testnet_rest_base": base_settings.kraken_testnet_rest_base,
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
    "kraken": {
        "KRAKEN_BASE_URL": "kraken_base_url",
        "KRAKEN_API_KEY": "kraken_api_key",
        "KRAKEN_SECRET_KEY": "kraken_secret_key",
        "KRAKEN_USE_TESTNET": "kraken_use_testnet",
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


# DEFAULT_SETTINGS audit (Raspberry/Kraken runtime):
# - active: read by API/services/loops in the current backend runtime.
# - legacy_read_only: accepted from existing app_settings/bootstrap rows for compatibility,
#   but intentionally hidden from /api/v1/admin/settings.
# - removable: obsolete persisted aliases/rows that can be backed up and deleted.
DEFAULT_SETTINGS_SECTION_AUDIT: dict[str, str] = {
    "general": "active",
    "executor": "active",
    "kraken": "active",
    "market_data": "active",
    "strategy": "active",
    "notifications": "active",
    "bot": "active",
    "live": "active",
    "momentum": "active",
}
DEFAULT_SETTINGS_KEY_AUDIT: dict[str, dict[str, str]] = {
    section: {key: DEFAULT_SETTINGS_SECTION_AUDIT.get(section, "legacy_read_only") for key in values}
    for section, values in DEFAULT_SETTINGS.items()
}

LEGACY_READ_ONLY_APP_SETTING_KEYS: frozenset[tuple[str, str]] = frozenset(
    {
        ("admin/security", "ADMIN_TOKEN"),
        ("kraken", "EXECUTION_EXCHANGE"),
        ("kraken", "kraken_rest_base"),
        ("kraken", "KRAKEN_BASE_URL"),
        ("kraken", "KRAKEN_API_KEY"),
        ("kraken", "KRAKEN_SECRET_KEY"),
        ("kraken", "KRAKEN_USE_TESTNET"),
    }
)

REMOVABLE_LEGACY_APP_SETTING_KEYS: frozenset[tuple[str, str]] = frozenset(
    LEGACY_READ_ONLY_APP_SETTING_KEYS
    | {
        (section, display_key)
        for section, aliases in ADMIN_FIELD_ALIASES.items()
        for display_key in aliases
    }
)

CRITICAL_APP_SETTING_KEYS: frozenset[tuple[str, str]] = frozenset(
    {
        ("general", "admin_token"),
        ("executor", "execution_exchange"),
        ("executor", "quote_assets"),
        ("kraken", "kraken_base_url"),
        ("kraken", "kraken_api_key"),
        ("kraken", "kraken_secret_key"),
        ("notifications", "telegram_chat_id"),
        ("notifications", "telegram_secret"),
        ("notifications", "discord_url"),
        ("live", "live_trading_enabled"),
    }
)


def _has_value(value: Any) -> bool:
    return value is not None and str(value) != ""


def _migrate_legacy_kraken_base_url_rows(db: Session, rows: list[AppSetting]) -> list[AppSetting]:
    """Rename persisted kraken_rest_base rows to canonical kraken_base_url rows.

    The legacy row is read only as migration input and is deleted afterwards so
    admin/runtime payloads expose only kraken_base_url. Existing canonical values
    win over legacy values unless they are empty.
    """
    by_location = {(row.category, row.key): row for row in rows}
    legacy_row = by_location.get(("kraken", "kraken_rest_base"))
    if legacy_row is None:
        return rows

    canonical_row = by_location.get(("kraken", "kraken_base_url"))
    changed = False
    if canonical_row is None and _has_value(legacy_row.value):
        canonical_row = AppSetting(category="kraken", key="kraken_base_url", value=legacy_row.value)
        db.add(canonical_row)
        rows.append(canonical_row)
        changed = True
    elif canonical_row is not None and not _has_value(canonical_row.value) and _has_value(legacy_row.value):
        canonical_row.value = legacy_row.value
        changed = True

    db.delete(legacy_row)
    if legacy_row in rows:
        rows.remove(legacy_row)
    changed = True

    if changed:
        db.commit()
        rows = db.execute(select(AppSetting)).scalars().all()
    return rows

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


def _legacy_bootstrap_values() -> dict[str, Any]:
    values: dict[str, Any] = {}
    try:
        from raspberry_executor.env_store import ENV_PATH

        if ENV_PATH.exists():
            for raw in ENV_PATH.read_text().splitlines():
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()
    except Exception:
        pass
    values.update({key: os.environ[key] for key in LEGACY_RASPBERRY_SETTING_ALIASES if key in os.environ})
    try:
        from raspberry_executor.sqlite_db import DB_PATH
        from raspberry_executor.settings_store import read_settings

        if DB_PATH.exists():
            values.update(read_settings())
    except Exception:
        pass
    return values


def migrate_bootstrap_settings_to_app_settings(db: Session, rows: list[AppSetting]) -> list[AppSetting]:
    """Copy useful legacy .env/SQLite settings into canonical app_settings rows.

    This is intentionally non-destructive: canonical app_settings values always win.
    The bootstrap stores are read only to fill missing or empty canonical rows before
    they can be fully retired from runtime writes.
    """
    by_location = {(row.category, row.key): row for row in rows}
    bootstrap_values = _legacy_bootstrap_values()
    changed = False
    for source_key, target in LEGACY_RASPBERRY_SETTING_ALIASES.items():
        source_value = bootstrap_values.get(source_key)
        if not _has_value(source_value):
            continue
        target_row = by_location.get(target)
        if target_row is None:
            target_row = AppSetting(category=target[0], key=target[1], value=source_value)
            db.add(target_row)
            by_location[target] = target_row
            rows.append(target_row)
            changed = True
        elif not _has_value(target_row.value):
            target_row.value = source_value
            changed = True
    if changed:
        db.commit()
        rows = db.execute(select(AppSetting)).scalars().all()
    return rows


def _apply_legacy_admin_setting_locations(payload: dict[str, dict[str, Any]]) -> None:
    """Keep old persisted admin rows working after splitting executor/market data settings."""
    kraken = payload.setdefault("kraken", {})
    executor = payload.setdefault("executor", {})
    market_data = payload.setdefault("market_data", {})

    for legacy_key in ("execution_exchange", "EXECUTION_EXCHANGE"):
        if legacy_key in kraken:
            executor["execution_exchange"] = kraken.pop(legacy_key)
    for legacy_key in ("kraken_quote_assets", "KRAKEN_QUOTE_ASSETS"):
        if legacy_key in kraken:
            executor["quote_assets"] = kraken.get(legacy_key)
    market_data_keys = {key for key in DEFAULT_SETTINGS["market_data"] if key != "kraken_quote_assets"}
    market_data_keys.add("kraken_quote_assets")
    for key in list(kraken.keys()):
        if key in market_data_keys:
            market_data[key] = kraken.pop(key)
    market_data.setdefault("kraken_quote_assets", executor.get("quote_assets", base_settings.kraken_quote_assets))

def _canonical_admin_field(section: str, key: str) -> tuple[str, str]:
    aliases = ADMIN_FIELD_ALIASES.get(section, {})
    if key in aliases:
        target_section = "general" if section == "admin/security" else section
        return target_section, aliases[key]
    return section, key


def _with_admin_aliases(payload: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    admin_payload = {section: values.copy() for section, values in payload.items()}
    for section in ("general", "executor", "kraken", "market_data", "strategy", "notifications", "bot", "live", "momentum", "admin/security"):
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
    "executor": tuple(DEFAULT_SETTINGS["executor"].keys()),
    "kraken": ("kraken_exchange_name", "kraken_base_url", "kraken_api_key", "kraken_secret_key"),
    "market_data": tuple(DEFAULT_SETTINGS["market_data"].keys()),
    "strategy": tuple(DEFAULT_SETTINGS["strategy"].keys()),
    "notifications": tuple(DEFAULT_SETTINGS["notifications"].keys()),
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
            rows = _migrate_legacy_kraken_base_url_rows(db, rows)
            rows = _migrate_legacy_admin_setting_rows(db, rows)
            rows = migrate_bootstrap_settings_to_app_settings(db, rows)
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
        payload.setdefault("market_data", {})["kraken_collector_enabled"] = bool(payload.get("market_data", {}).get("kraken_collector_enabled", True))
        _apply_legacy_admin_setting_locations(payload)
        kraken = payload.setdefault("kraken", {})
        if not kraken.get("kraken_base_url") and kraken.get("kraken_rest_base"):
            kraken["kraken_base_url"] = kraken["kraken_rest_base"]
        kraken.pop("kraken_rest_base", None)
        payload.setdefault("momentum", {})["momentum_candidates_sync_enabled"] = bool(payload.get("momentum", {}).get("momentum_candidates_sync_enabled", False))
        payload.setdefault("momentum", {})["momentum_candidates_require_wyckoff_context"] = bool(payload.get("momentum", {}).get("momentum_candidates_require_wyckoff_context", True))
        return payload
    finally:
        if owns_session:
            db.close()


def cleanup_legacy_app_settings(
    db: Session,
    backup_path: str | Path | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Back up app_settings then delete obsolete legacy rows.

    Canonical rows and active runtime keys are preserved.  Legacy rows remain
    readable through load_runtime_settings migrations until this explicit cleanup
    command is run.
    """
    rows = db.execute(select(AppSetting)).scalars().all()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if backup_path is None:
        backup = Path("backups") / f"app_settings_before_legacy_cleanup_{timestamp}.json"
    else:
        backup = Path(backup_path)
        if backup.is_dir():
            backup = backup / f"app_settings_before_legacy_cleanup_{timestamp}.json"
    backup.parent.mkdir(parents=True, exist_ok=True)

    serialized_rows = [
        {"category": row.category, "key": row.key, "value": row.value}
        for row in sorted(rows, key=lambda row: (row.category, row.key))
    ]
    critical_rows = [
        row for row in serialized_rows if (row["category"], row["key"]) in CRITICAL_APP_SETTING_KEYS
    ]
    legacy_rows = [
        row for row in rows if (row.category, row.key) in REMOVABLE_LEGACY_APP_SETTING_KEYS
    ]
    backup.write_text(
        json.dumps(
            {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "dry_run": dry_run,
                "critical_keys": sorted([f"{section}.{key}" for section, key in CRITICAL_APP_SETTING_KEYS]),
                "critical_rows": critical_rows,
                "rows": serialized_rows,
                "legacy_rows_to_delete": [
                    {"category": row.category, "key": row.key, "value": row.value}
                    for row in sorted(legacy_rows, key=lambda row: (row.category, row.key))
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )

    if not dry_run:
        for row in legacy_rows:
            db.delete(row)
        db.commit()

    return {
        "backup_path": str(backup),
        "dry_run": dry_run,
        "deleted_count": 0 if dry_run else len(legacy_rows),
        "matched_legacy_count": len(legacy_rows),
        "deleted_keys": sorted({f"{row.category}.{row.key}" for row in legacy_rows}),
    }


def persist_runtime_settings(db: Session, payload: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    payload = _normalize_admin_payload(payload)
    kraken_payload = payload.setdefault("kraken", {})
    if not kraken_payload.get("kraken_base_url") and kraken_payload.get("kraken_rest_base"):
        kraken_payload["kraken_base_url"] = kraken_payload["kraken_rest_base"]
    kraken_payload.pop("kraken_rest_base", None)
    db.execute(delete(AppSetting).where(AppSetting.category == "kraken", AppSetting.key == "kraken_rest_base"))
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

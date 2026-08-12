from __future__ import annotations

from copy import deepcopy
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.core.config import settings as base_settings
from app.db.session import SessionLocal
from app.models.app_setting import AppSetting


MOMENTUM_CADENCE_KEY = "momentum_engine_cadence_hours"
SUPPORTED_MOMENTUM_CADENCES = {1, 4, 8, 24}
STOCK_ETF_TIMEFRAMES = {"15m", "1h", "4h", "1d"}
STOCK_ETF_ASSET_TYPES = {"STOCK", "ETF"}


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


def _momentum_cadence(value: Any, default: int = 1) -> int:
    try:
        cadence = int(value)
    except (TypeError, ValueError):
        return default
    return cadence if cadence in SUPPORTED_MOMENTUM_CADENCES else default


DEFAULT_SETTINGS: dict[str, dict[str, Any]] = {
    "general": {
        "app_name": base_settings.app_name,
        "app_env": base_settings.app_env,
        "cors_origins": base_settings.cors_origins,
        "create_tables_on_boot": base_settings.create_tables_on_boot,
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
        "bot_pipeline_symbol_limit": "all",
        "bot_pipeline_interval_sec": base_settings.bot_pipeline_interval_sec,
        "bot_executor_interval_sec": base_settings.bot_executor_interval_sec,
        "bot_scheduler_interval_sec": base_settings.bot_scheduler_interval_sec,
        "bot_momentum_engine_interval_sec": base_settings.bot_momentum_engine_interval_sec,
        "bot_executor_limit": base_settings.bot_executor_limit,
        "bot_executor_quantity": base_settings.bot_executor_quantity,
    },
    "momentum": {
        "momentum_engine_enabled": True,
        "momentum_engine_interval_sec": base_settings.bot_momentum_engine_interval_sec,
        "momentum_engine_cadence_hours": _momentum_cadence(base_settings.momentum_engine_cadence_hours),
        "momentum_engine_starting_capital": 1000.0,
        "momentum_engine_min_score": 0.0,
    },
    "stock_etf": {
        # Opt-in defaults prevent a new installation from starting data imports or trades.
        "feeder_enabled": False,
        "momentum_enabled": False,
        "wyckoff_smc_enabled": False,
        "momentum_cadence_hours": 24,
        "wyckoff_smc_cadence_hours": 1,
        "universes": ["Europe Stocks", "Europe ETF"],
        "asset_types": ["STOCK", "ETF"],
        "timeframes": ["1d"],
        "exchange_timezone": "Europe/Paris",
        "market_open": "09:00",
        "market_close": "17:30",
        "min_lot_size": 1,
        "max_lot_size": 100,
        "retry_max_attempts": 3,
        "retry_delay_seconds": 30,
        "timeout_seconds": 1800,
        "paper_momentum": {
            "enabled": False,
            "starting_capital": 1000.0,
            "reference_currency": "EUR",
            "max_positions": 1,
            "max_position_pct": 10.0,
        },
    },
    "scheduler": {"reconciliation_interval_seconds": 300, "abandoned_after_seconds": 900},
    "live": {
        "live_spot_allow_shorts": base_settings.live_spot_allow_shorts,
        "live_max_open_positions": base_settings.live_max_open_positions,
        "live_max_notional_per_trade": base_settings.live_max_notional_per_trade,
        "live_require_tp_sl": base_settings.live_require_tp_sl,
        "live_reconcile_enabled": base_settings.live_reconcile_enabled,
    },
}

# Runtime values have one deliberately simple precedence order:
#
#     explicit AppSetting row > value loaded by Settings from .env/process env
#     > built-in Settings/default value
#
# AppSetting rows are therefore overrides, never a cache of the resolved
# configuration.  In particular, partial writes must not materialize inherited
# values in the database, or a later .env change would be silently masked.
def _migrate_stock_etf(rows: list[AppSetting], stock_etf: dict[str, Any]) -> None:
    """Read legacy stock/ETF categories without borrowing any crypto setting."""
    explicit = {(row.category, row.key) for row in rows}
    legacy = {row.category: {} for row in rows if row.category.startswith("stock_etf_")}
    for row in rows:
        if row.category in legacy:
            legacy[row.category][row.key] = row.value
    momentum = legacy.get("stock_etf_momentum", {})
    wyckoff = legacy.get("stock_etf_wyckoff_smc", {})
    mappings = (
        ("momentum_enabled", momentum, "enabled"),
        ("momentum_cadence_hours", momentum, "cadence_hours"),
        ("wyckoff_smc_enabled", wyckoff, "enabled"),
        ("wyckoff_smc_cadence_hours", wyckoff, "cadence_hours"),
    )
    for target, source, old_key in mappings:
        if ("stock_etf", target) not in explicit and old_key in source:
            stock_etf[target] = source[old_key]
    for key in ("universes", "asset_types", "exchange_timezone", "market_open", "market_close", "timeout_seconds"):
        if ("stock_etf", key) not in explicit:
            if key in momentum:
                stock_etf[key] = momentum[key]
            elif key in wyckoff:
                stock_etf[key] = wyckoff[key]
    if ("stock_etf", "timeframes") not in explicit:
        migrated_timeframes = list(dict.fromkeys([
            *momentum.get("timeframes", []),
            *wyckoff.get("timeframes", []),
        ]))
        if migrated_timeframes:
            stock_etf["timeframes"] = migrated_timeframes
    paper = stock_etf.setdefault("paper_momentum", {})
    for old_key, target in (("starting_capital", "starting_capital"), ("reference_currency", "reference_currency"), ("max_positions", "max_positions")):
        if old_key in momentum and ("stock_etf", "paper_momentum") not in explicit:
            paper[target] = momentum[old_key]


def validate_stock_etf_settings(config: dict[str, Any]) -> None:
    errors: list[str] = []
    timeframes = config.get("timeframes")
    if not isinstance(timeframes, list) or not timeframes or any(item not in STOCK_ETF_TIMEFRAMES for item in timeframes):
        errors.append("timeframes must be a non-empty subset of 15m, 1h, 4h and 1d")
        timeframes = []
    if config.get("momentum_enabled") and "1d" not in timeframes:
        errors.append("Momentum requires the feeder timeframe 1d")
    if config.get("wyckoff_smc_enabled") and not {"15m", "1h", "4h"}.issubset(timeframes):
        errors.append("Wyckoff/SMC requires feeder timeframes 15m, 1h and 4h")
    if (config.get("momentum_enabled") or config.get("wyckoff_smc_enabled")) and not config.get("feeder_enabled"):
        errors.append("an enabled stock/ETF workflow requires the feeder")
    if not config.get("universes"):
        errors.append("at least one stock/ETF universe is required")
    asset_types = config.get("asset_types")
    if not isinstance(asset_types, list) or not asset_types or not set(asset_types).issubset(STOCK_ETF_ASSET_TYPES):
        errors.append("asset_types must contain STOCK and/or ETF")
    for key in ("momentum_cadence_hours", "wyckoff_smc_cadence_hours", "min_lot_size", "max_lot_size", "retry_max_attempts", "retry_delay_seconds"):
        try:
            if float(config.get(key, 0)) <= 0:
                errors.append(f"{key} must be greater than zero")
        except (TypeError, ValueError):
            errors.append(f"{key} must be numeric")
    if isinstance(config.get("min_lot_size"), (int, float)) and isinstance(config.get("max_lot_size"), (int, float)) and config["min_lot_size"] > config["max_lot_size"]:
        errors.append("min_lot_size cannot exceed max_lot_size")
    paper = config.get("paper_momentum")
    if not isinstance(paper, dict):
        errors.append("paper_momentum must be an object")
    elif paper.get("enabled"):
        if not config.get("momentum_enabled"):
            errors.append("the Momentum paper portfolio requires the Momentum engine")
        if float(paper.get("starting_capital", 0) or 0) <= 0 or int(paper.get("max_positions", 0) or 0) <= 0:
            errors.append("paper Momentum capital and max positions must be greater than zero")
    if errors:
        raise ValueError("; ".join(errors))


def load_runtime_settings(db: Session | None = None) -> dict[str, dict[str, Any]]:
    owns_session = db is None
    if db is None:
        db = SessionLocal()
    try:
        rows = db.execute(select(AppSetting)).scalars().all()
        payload = deepcopy(DEFAULT_SETTINGS)
        for row in rows:
            payload.setdefault(row.category, {})[row.key] = row.value
        _migrate_stock_etf(rows, payload["stock_etf"])
        # Legacy rows remain in the database for rollback compatibility, but the
        # public schema exposes one unambiguous namespace.
        payload.pop("stock_etf_momentum", None)
        payload.pop("stock_etf_wyckoff_smc", None)
        strategy = payload.setdefault("strategy", {})
        strategy["signal_execution_interval"] = "15m"
        strategy["signal_entry_rsi_timeframe"] = _entry_rsi_timeframe(strategy.get("signal_entry_rsi_timeframe"))
        payload.setdefault("bot", {})["bot_momentum_engine_enabled"] = _as_bool(
            payload.get("bot", {}).get("bot_momentum_engine_enabled", True),
            default=True,
        )
        momentum = payload.setdefault("momentum", {})
        momentum["momentum_engine_enabled"] = _as_bool(
            momentum.get("momentum_engine_enabled", True),
            default=True,
        )
        momentum[MOMENTUM_CADENCE_KEY] = _momentum_cadence(momentum.get(MOMENTUM_CADENCE_KEY))
        return payload
    finally:
        if owns_session:
            db.close()


def load_runtime_settings_admin(db: Session) -> dict[str, Any]:
    """Return effective settings and override identifiers, without source labels."""
    rows = db.execute(select(AppSetting)).scalars().all()
    return {
        "settings": load_runtime_settings(db),
        "overrides": [{"category": row.category, "key": row.key} for row in rows],
    }


def delete_runtime_setting_override(db: Session, category: str, key: str) -> dict[str, Any]:
    """Delete one explicit override so the effective value falls back to env/default."""
    db.execute(delete(AppSetting).where(AppSetting.category == category, AppSetting.key == key))
    db.commit()
    return load_runtime_settings_admin(db)


def persist_runtime_settings(db: Session, payload: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    strategy = payload.get("strategy")
    if isinstance(strategy, dict):
        if "signal_execution_interval" in strategy:
            strategy["signal_execution_interval"] = "15m"
        if "signal_entry_rsi_timeframe" in strategy:
            strategy["signal_entry_rsi_timeframe"] = _entry_rsi_timeframe(strategy["signal_entry_rsi_timeframe"])

    bot = payload.get("bot")
    if isinstance(bot, dict) and "bot_momentum_engine_enabled" in bot:
        bot["bot_momentum_engine_enabled"] = _as_bool(bot["bot_momentum_engine_enabled"], default=True)

    momentum = payload.get("momentum")
    if isinstance(momentum, dict):
        if "momentum_engine_enabled" in momentum:
            momentum["momentum_engine_enabled"] = _as_bool(momentum["momentum_engine_enabled"], default=True)
        if MOMENTUM_CADENCE_KEY in momentum:
            momentum[MOMENTUM_CADENCE_KEY] = _momentum_cadence(momentum[MOMENTUM_CADENCE_KEY])

    stock_etf = payload.get("stock_etf")
    if isinstance(stock_etf, dict):
        merged_stock_etf = deepcopy(load_runtime_settings(db)["stock_etf"])
        merged_stock_etf.update(stock_etf)
        merged_paper = deepcopy(merged_stock_etf["paper_momentum"])
        merged_paper.update(stock_etf.get("paper_momentum", {}))
        merged_stock_etf["paper_momentum"] = merged_paper
        validate_stock_etf_settings(merged_stock_etf)
        # Validate the effective configuration, but persist only fields supplied
        # by the caller below.

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
            "min": strategy.get("signal_entry_rsi_min", 50.0),
            "max": strategy.get("signal_entry_rsi_max", 65.0),
            "timeframe": strategy.get("signal_entry_rsi_timeframe", "1h"),
        },
        "signals": {
            "overbought": strategy["signal_overbought"],
            "oversold": strategy["signal_oversold"],
            "price_near_extreme_pct": strategy["signal_price_near_extreme_pct"],
        },
    }

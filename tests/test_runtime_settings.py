from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool
from pathlib import Path

from app.models.app_setting import AppSetting
from app.models.base import Base
from app.core.config import Settings
import pytest

from app.services.runtime_settings import (
    DEFAULT_SETTINGS,
    get_runtime_signal_config,
    delete_runtime_setting_override,
    load_runtime_settings,
    load_runtime_settings_admin,
    persist_runtime_settings,
)
from scripts.run_pipeline_loop import parse_pipeline_interval
SIGNAL_ENV_KEYS = (
    "SIGNAL_SESSION_CONFIRM_FILTER_ENABLED",
    "SIGNAL_ENTRY_RSI_MIN",
    "SIGNAL_ENTRY_RSI_MAX",
    "PLANNER_MIN_RR",
    "PLANNER_MIN_SCORE",
)


def _session() -> Session:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return Session(engine)


def test_signal_configuration_defaults_and_environment_overrides(monkeypatch) -> None:
    for key in SIGNAL_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)

    defaults = Settings(_env_file=None)

    assert defaults.signal_session_confirm_filter_enabled is False
    assert defaults.signal_entry_rsi_min == 50.0
    assert defaults.signal_entry_rsi_max == 65.0
    assert defaults.planner_min_rr == 1.75
    assert defaults.planner_min_score == 25.0
    monkeypatch.setattr(
        "app.services.runtime_settings.load_runtime_settings",
        lambda db=None: {"strategy": defaults.model_dump()},
    )
    default_runtime = get_runtime_signal_config()
    assert default_runtime["session_confirm_filter_enabled"] is False
    assert default_runtime["entry_rsi"] == {"min": 50.0, "max": 65.0, "timeframe": "1h"}

    overrides = {
        "SIGNAL_SESSION_CONFIRM_FILTER_ENABLED": "true",
        "SIGNAL_ENTRY_RSI_MIN": "52.5",
        "SIGNAL_ENTRY_RSI_MAX": "67.5",
        "PLANNER_MIN_RR": "2.25",
        "PLANNER_MIN_SCORE": "30",
    }
    for key, value in overrides.items():
        monkeypatch.setenv(key, value)

    configured = Settings(_env_file=None)

    assert configured.signal_session_confirm_filter_enabled is True
    assert configured.signal_entry_rsi_min == 52.5
    assert configured.signal_entry_rsi_max == 67.5
    assert configured.planner_min_rr == 2.25
    assert configured.planner_min_score == 30.0
    monkeypatch.setattr(
        "app.services.runtime_settings.load_runtime_settings",
        lambda db=None: {"strategy": configured.model_dump()},
    )
    configured_runtime = get_runtime_signal_config()
    assert configured_runtime["session_confirm_filter_enabled"] is True
    assert configured_runtime["entry_rsi"] == {"min": 52.5, "max": 67.5, "timeframe": "1h"}


def test_runtime_signal_config_uses_new_rsi_fallback(monkeypatch) -> None:
    strategy = Settings(_env_file=None).model_dump()
    strategy.pop("signal_entry_rsi_min")
    monkeypatch.setattr(
        "app.services.runtime_settings.load_runtime_settings",
        lambda db=None: {"strategy": strategy},
    )

    runtime = get_runtime_signal_config()

    assert runtime["session_confirm_filter_enabled"] is False
    assert runtime["entry_rsi"] == {"min": 50.0, "max": 65.0, "timeframe": "1h"}


def test_load_runtime_settings_defaults_momentum_cadence_to_one_hour() -> None:
    with _session() as db:
        runtime = load_runtime_settings(db)

    assert runtime["momentum"]["momentum_paper_cadence_hours"] == 1
    # Polling stays more frequent than the user-selected cadence so a structure
    # break can make a due-only run sell immediately.
    assert runtime["momentum"]["momentum_paper_interval_sec"] == 300
    assert runtime["bot"]["bot_momentum_paper_interval_sec"] == 300


def test_pipeline_interval_defaults_to_fifteen_minutes() -> None:
    assert Settings(_env_file=None).bot_pipeline_interval_sec == 900
    assert DEFAULT_SETTINGS["bot"]["bot_pipeline_interval_sec"] == 900


def test_valid_pipeline_interval_is_converted_and_persisted() -> None:
    with _session() as db:
        runtime = persist_runtime_settings(db, {"bot": {"bot_pipeline_interval_sec": "120"}})
        row = db.execute(select(AppSetting)).scalar_one()

    assert runtime["bot"]["bot_pipeline_interval_sec"] == 120
    assert row.value == 120


@pytest.mark.parametrize("interval", [None, -1, 0, 59, "not-an-integer"])
def test_invalid_pipeline_interval_is_rejected(interval) -> None:
    with _session() as db, pytest.raises(ValueError, match=r"bot\.bot_pipeline_interval_sec.*at least 60"):
        persist_runtime_settings(db, {"bot": {"bot_pipeline_interval_sec": interval}})


@pytest.mark.parametrize(("legacy_value", "effective"), [
    (None, 900),
    ("invalid", 900),
    (-10, 60),
    (0, 60),
    (59, 60),
    (120, 120),
])
def test_pipeline_worker_defensively_bounds_legacy_intervals(legacy_value, effective) -> None:
    assert parse_pipeline_interval(legacy_value) == effective


def test_momentum_interval_default_can_be_configured_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("BOT_MOMENTUM_PAPER_INTERVAL_SEC", "7200")

    configured = Settings(_env_file=None)

    assert configured.bot_momentum_paper_interval_sec == 7200


def test_momentum_cadence_defaults_to_one_hour_and_can_be_configured_from_environment(monkeypatch) -> None:
    assert Settings(_env_file=None).momentum_paper_cadence_hours == 1

    monkeypatch.setenv("MOMENTUM_PAPER_CADENCE_HOURS", "4")

    assert Settings(_env_file=None).momentum_paper_cadence_hours == 4


def test_load_runtime_settings_honors_persisted_four_hour_cadence() -> None:
    with _session() as db:
        db.add(AppSetting(
            category="momentum",
            key="momentum_paper_cadence_hours",
            value=4,
        ))
        db.commit()

        runtime = load_runtime_settings(db)
        assert runtime["momentum"]["momentum_paper_cadence_hours"] == 4


def test_load_runtime_settings_honors_explicit_supported_cadence() -> None:
    with _session() as db:
        persist_runtime_settings(db, {"momentum": {"momentum_paper_cadence_hours": 4}})

        runtime = load_runtime_settings(db)

    assert runtime["momentum"]["momentum_paper_cadence_hours"] == 4


def test_environment_value_is_effective_without_database_row(monkeypatch) -> None:
    monkeypatch.setitem(DEFAULT_SETTINGS["strategy"], "planner_min_score", 31.0)
    with _session() as db:
        response = load_runtime_settings_admin(db)
    assert response["settings"]["strategy"]["planner_min_score"] == 31.0
    assert response["overrides"] == []


def test_database_override_wins_and_can_be_deleted() -> None:
    with _session() as db:
        persist_runtime_settings(db, {"strategy": {"planner_min_score": 40}})
        response = load_runtime_settings_admin(db)
        assert response["settings"]["strategy"]["planner_min_score"] == 40
        assert response["overrides"] == [{"category": "strategy", "key": "planner_min_score"}]

        response = delete_runtime_setting_override(db, "strategy", "planner_min_score")
        assert response["settings"]["strategy"]["planner_min_score"] == DEFAULT_SETTINGS["strategy"]["planner_min_score"]
        assert response["overrides"] == []


def test_partial_update_does_not_materialize_defaults() -> None:
    with _session() as db:
        persist_runtime_settings(db, {"strategy": {"planner_min_score": 33}})
        rows = db.execute(select(AppSetting)).scalars().all()
    assert [(row.category, row.key) for row in rows] == [("strategy", "planner_min_score")]


def test_sql_migration_removes_legacy_fingerprint_but_preserves_customization() -> None:
    migration = Path("migrations/20260812_remove_legacy_strategy_defaults.sql").read_text()
    legacy = {
        "signal_entry_rsi_min": 45, "signal_entry_rsi_max": 55,
        "planner_min_score": 4, "planner_min_rr": 0.8,
        "signal_session_confirm_filter_enabled": True,
    }
    with _session() as db:
        db.add_all(AppSetting(category="strategy", key=key, value=value) for key, value in legacy.items())
        db.commit()
        db.connection().connection.executescript(migration)
        db.commit()
        assert db.execute(select(AppSetting)).scalars().all() == []

    with _session() as db:
        customized = {**legacy, "planner_min_score": 9}
        db.add_all(AppSetting(category="strategy", key=key, value=value) for key, value in customized.items())
        db.commit()
        db.connection().connection.executescript(migration)
        db.commit()
        assert len(db.execute(select(AppSetting)).scalars().all()) == 5


def test_stock_etf_defaults_are_safe_and_independent_from_crypto() -> None:
    with _session() as db:
        runtime = load_runtime_settings(db)

    assert runtime["stock_etf"]["feeder_enabled"] is False
    assert runtime["stock_etf"]["momentum_enabled"] is False
    assert runtime["stock_etf"]["wyckoff_smc_enabled"] is False
    assert runtime["stock_etf"]["timeframes"] == ["1d"]
    assert runtime["momentum"]["momentum_paper_enabled"] is True


def test_legacy_worker_names_are_read_through_clear_paper_names() -> None:
    with _session() as db:
        db.add_all([
            AppSetting(category="bot", key="bot_executor_limit", value=7),
            AppSetting(category="bot", key="bot_momentum_engine_enabled", value=False),
            AppSetting(category="momentum", key="momentum_engine_cadence_hours", value=4),
        ])
        db.commit()
        runtime = load_runtime_settings(db)

    assert runtime["bot"]["bot_wyckoff_paper_limit"] == 7
    assert runtime["bot"]["bot_momentum_paper_enabled"] is False
    assert runtime["momentum"]["momentum_paper_cadence_hours"] == 4
    assert "bot_executor_limit" not in runtime["bot"]
    assert "momentum_engine_cadence_hours" not in runtime["momentum"]


def test_stock_etf_update_round_trip_does_not_change_crypto_settings() -> None:
    with _session() as db:
        persist_runtime_settings(db, {"momentum": {"momentum_paper_cadence_hours": 8}})
        runtime = persist_runtime_settings(db, {"stock_etf": {
            "feeder_enabled": True,
            "momentum_enabled": True,
            "timeframes": ["1d"],
            "universes": ["Europe ETF"],
            "asset_types": ["ETF"],
            "max_lot_size": 25,
        }})

    assert runtime["stock_etf"]["max_lot_size"] == 25
    assert runtime["stock_etf"]["momentum_enabled"] is True
    assert runtime["momentum"]["momentum_paper_cadence_hours"] == 8


def test_legacy_stock_etf_settings_are_migrated_without_crypto_fallback() -> None:
    with _session() as db:
        db.add_all([
            AppSetting(category="stock_etf_momentum", key="enabled", value=True),
            AppSetting(category="stock_etf_momentum", key="cadence_hours", value=4),
            AppSetting(category="stock_etf_momentum", key="starting_capital", value=2500),
            AppSetting(category="momentum", key="momentum_paper_cadence_hours", value=8),
        ])
        db.commit()
        runtime = load_runtime_settings(db)

    assert runtime["stock_etf"]["momentum_enabled"] is True
    assert runtime["stock_etf"]["momentum_cadence_hours"] == 4
    assert runtime["stock_etf"]["paper_momentum"]["starting_capital"] == 2500
    assert runtime["momentum"]["momentum_paper_cadence_hours"] == 8


@pytest.mark.parametrize("stock_etf, message", [
    ({"feeder_enabled": True, "momentum_enabled": True, "timeframes": ["1h"]}, "Momentum requires"),
    ({"feeder_enabled": True, "wyckoff_smc_enabled": True, "timeframes": ["15m", "1h"]}, "Wyckoff/SMC requires"),
    ({"momentum_enabled": True, "timeframes": ["1d"]}, "requires the feeder"),
    ({"min_lot_size": 10, "max_lot_size": 2}, "cannot exceed"),
])
def test_stock_etf_invalid_combinations_are_rejected(stock_etf, message) -> None:
    with _session() as db, pytest.raises(ValueError, match=message):
        persist_runtime_settings(db, {"stock_etf": stock_etf})


def test_deployment_defaults_have_one_canonical_settings_source() -> None:
    """Every deployment-tunable runtime default must be read from Settings."""
    configured = Settings(_env_file=None)
    paths = {
        ("strategy", "signal_execution_interval"): "signal_execution_interval",
        ("bot", "bot_pipeline_interval_sec"): "bot_pipeline_interval_sec",
        ("bot", "bot_wyckoff_paper_interval_sec"): "bot_wyckoff_paper_interval_sec",
        ("bot", "bot_scheduler_interval_sec"): "bot_scheduler_interval_sec",
        ("momentum", "momentum_paper_starting_capital"): "momentum_paper_starting_capital",
        ("momentum", "momentum_paper_min_score"): "momentum_paper_min_score",
        ("stock_etf", "momentum_cadence_hours"): "stock_etf_momentum_cadence_hours",
        ("stock_etf", "wyckoff_smc_cadence_hours"): "stock_etf_wyckoff_smc_cadence_hours",
        ("stock_etf", "exchange_timezone"): "stock_etf_exchange_timezone",
        ("stock_etf", "market_open"): "stock_etf_market_open",
        ("stock_etf", "market_close"): "stock_etf_market_close",
        ("stock_etf", "retry_max_attempts"): "stock_etf_retry_max_attempts",
        ("stock_etf", "retry_delay_seconds"): "stock_etf_retry_delay_seconds",
        ("stock_etf", "timeout_seconds"): "stock_etf_timeout_seconds",
        ("scheduler", "reconciliation_interval_seconds"): "scheduler_reconciliation_interval_seconds",
        ("scheduler", "abandoned_after_seconds"): "scheduler_abandoned_after_seconds",
    }
    for (category, key), field in paths.items():
        assert DEFAULT_SETTINGS[category][key] == getattr(configured, field)
    assert DEFAULT_SETTINGS["stock_etf"]["paper_momentum"]["starting_capital"] == configured.stock_etf_paper_starting_capital
    assert DEFAULT_SETTINGS["stock_etf"]["paper_momentum"]["max_positions"] == configured.stock_etf_paper_max_positions
    assert DEFAULT_SETTINGS["stock_etf"]["paper_momentum"]["max_position_pct"] == configured.stock_etf_paper_max_position_pct


def test_execution_interval_is_normalized_once_and_consumed_by_services(monkeypatch) -> None:
    custom = Settings(_env_file=None, SIGNAL_EXECUTION_INTERVAL="1h")
    assert custom.signal_config()["execution_interval"] == "1h"

    with _session() as db:
        runtime = persist_runtime_settings(db, {"strategy": {"signal_execution_interval": "4h"}})
        assert runtime["strategy"]["signal_execution_interval"] == "4h"
        assert get_runtime_signal_config(db)["execution_interval"] == "4h"

    monkeypatch.setattr(
        "app.services.signal_engine_service.get_runtime_signal_config",
        lambda: {"execution_interval": "1h"},
    )
    from app.services.signal_engine_service import SignalEngineService
    assert SignalEngineService().heartbeat()["primary_interval"] == "1h"

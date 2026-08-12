from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.models.app_setting import AppSetting
from app.models.base import Base
from app.core.config import Settings
import pytest

from app.services.runtime_settings import load_runtime_settings, persist_runtime_settings


def _session() -> Session:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return Session(engine)


def test_load_runtime_settings_defaults_momentum_cadence_to_one_hour() -> None:
    with _session() as db:
        runtime = load_runtime_settings(db)

    assert runtime["momentum"]["momentum_engine_cadence_hours"] == 1
    # Polling stays more frequent than the user-selected cadence so a structure
    # break can make a due-only run sell immediately.
    assert runtime["momentum"]["momentum_engine_interval_sec"] == 300
    assert runtime["bot"]["bot_momentum_engine_interval_sec"] == 300


def test_momentum_interval_default_can_be_configured_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("BOT_MOMENTUM_ENGINE_INTERVAL_SEC", "7200")

    configured = Settings(_env_file=None)

    assert configured.bot_momentum_engine_interval_sec == 7200


def test_momentum_cadence_defaults_to_one_hour_and_can_be_configured_from_environment(monkeypatch) -> None:
    assert Settings(_env_file=None).momentum_engine_cadence_hours == 1

    monkeypatch.setenv("MOMENTUM_ENGINE_CADENCE_HOURS", "4")

    assert Settings(_env_file=None).momentum_engine_cadence_hours == 4


def test_load_runtime_settings_honors_persisted_four_hour_cadence() -> None:
    with _session() as db:
        db.add(AppSetting(
            category="momentum",
            key="momentum_engine_cadence_hours",
            value=4,
        ))
        db.commit()

        runtime = load_runtime_settings(db)
        assert runtime["momentum"]["momentum_engine_cadence_hours"] == 4


def test_load_runtime_settings_honors_explicit_supported_cadence() -> None:
    with _session() as db:
        persist_runtime_settings(db, {"momentum": {"momentum_engine_cadence_hours": 4}})

        runtime = load_runtime_settings(db)

    assert runtime["momentum"]["momentum_engine_cadence_hours"] == 4


def test_stock_etf_defaults_are_safe_and_independent_from_crypto() -> None:
    with _session() as db:
        runtime = load_runtime_settings(db)

    assert runtime["stock_etf"]["feeder_enabled"] is False
    assert runtime["stock_etf"]["momentum_enabled"] is False
    assert runtime["stock_etf"]["wyckoff_smc_enabled"] is False
    assert runtime["stock_etf"]["timeframes"] == ["1d"]
    assert runtime["momentum"]["momentum_engine_enabled"] is True


def test_stock_etf_update_round_trip_does_not_change_crypto_settings() -> None:
    with _session() as db:
        persist_runtime_settings(db, {"momentum": {"momentum_engine_cadence_hours": 8}})
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
    assert runtime["momentum"]["momentum_engine_cadence_hours"] == 8


def test_legacy_stock_etf_settings_are_migrated_without_crypto_fallback() -> None:
    with _session() as db:
        db.add_all([
            AppSetting(category="stock_etf_momentum", key="enabled", value=True),
            AppSetting(category="stock_etf_momentum", key="cadence_hours", value=4),
            AppSetting(category="stock_etf_momentum", key="starting_capital", value=2500),
            AppSetting(category="momentum", key="momentum_engine_cadence_hours", value=8),
        ])
        db.commit()
        runtime = load_runtime_settings(db)

    assert runtime["stock_etf"]["momentum_enabled"] is True
    assert runtime["stock_etf"]["momentum_cadence_hours"] == 4
    assert runtime["stock_etf"]["paper_momentum"]["starting_capital"] == 2500
    assert runtime["momentum"]["momentum_engine_cadence_hours"] == 8


@pytest.mark.parametrize("stock_etf, message", [
    ({"feeder_enabled": True, "momentum_enabled": True, "timeframes": ["1h"]}, "Momentum requires"),
    ({"feeder_enabled": True, "wyckoff_smc_enabled": True, "timeframes": ["15m", "1h"]}, "Wyckoff/SMC requires"),
    ({"momentum_enabled": True, "timeframes": ["1d"]}, "requires the feeder"),
    ({"min_lot_size": 10, "max_lot_size": 2}, "cannot exceed"),
])
def test_stock_etf_invalid_combinations_are_rejected(stock_etf, message) -> None:
    with _session() as db, pytest.raises(ValueError, match=message):
        persist_runtime_settings(db, {"stock_etf": stock_etf})

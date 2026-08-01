from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.models.app_setting import AppSetting
from app.models.base import Base
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

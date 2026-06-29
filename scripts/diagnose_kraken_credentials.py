from __future__ import annotations

import os

from sqlalchemy import select

from app.db.session import SessionLocal
from app.models.app_setting import AppSetting
from app.services.runtime_settings import load_runtime_settings
from raspberry_executor.config import load_settings


def _status(value: object) -> str:
    if value is None or str(value) == "":
        return "EMPTY"
    return f"SET length={len(str(value))}"


def main() -> int:
    legacy = {}
    db = SessionLocal()
    try:
        rows = db.execute(
            select(AppSetting).where(
                AppSetting.category == "kraken",
                AppSetting.key.in_(["KRAKEN_API_KEY", "KRAKEN_SECRET_KEY"]),
            )
        ).scalars().all()
        legacy = {row.key: row.value for row in rows}
    finally:
        db.close()

    runtime = load_runtime_settings()
    kraken = runtime.get("kraken", {})
    settings_file = load_settings()
    db_api = kraken.get("kraken_api_key")
    db_secret = kraken.get("kraken_secret_key")
    env_api = os.environ.get("KRAKEN_API_KEY")
    env_secret = os.environ.get("KRAKEN_SECRET_KEY")

    print(f"DB kraken_api_key: {_status(db_api)}")
    print(f"DB kraken_secret_key: {_status(db_secret)}")
    print(f"DB KRAKEN_API_KEY legacy alias: IGNORED {_status(legacy.get('KRAKEN_API_KEY'))}")
    print(f"DB KRAKEN_SECRET_KEY legacy alias: IGNORED {_status(legacy.get('KRAKEN_SECRET_KEY'))}")
    print(f"ENV KRAKEN_API_KEY: {_status(env_api)}")
    print(f"ENV KRAKEN_SECRET_KEY: {_status(env_secret)}")
    print(f"Settings file KRAKEN_API_KEY: {_status(settings_file.kraken_api_key)}")
    print(f"Settings file KRAKEN_SECRET_KEY: {_status(settings_file.kraken_secret_key)}")

    if db_api and db_secret:
        selected = "database canonical lowercase"
    elif env_api and env_secret:
        selected = "environment"
    elif settings_file.kraken_api_key and settings_file.kraken_secret_key:
        selected = "settings file"
    else:
        selected = "none"
    ok = selected != "none"
    print(f"Selected source: {selected}")
    print(f"Result: {'OK credentials loaded' if ok else 'MISSING credentials'}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import os

from raspberry_executor.config import load_settings
from raspberry_executor.runtime_db_settings import load_runtime_settings_lightweight


def _status(value: object) -> str:
    if value is None or str(value) == "":
        return "EMPTY"
    return f"SET length={len(str(value))}"


def main() -> int:
    runtime, diagnostics = load_runtime_settings_lightweight()
    kraken = runtime.get("kraken", {})
    legacy = (diagnostics.get("legacy_aliases_seen") or {}).get("kraken", {})
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
    if diagnostics.get("db_error"):
        print(f"DB diagnostic error: {diagnostics['db_error']}")

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

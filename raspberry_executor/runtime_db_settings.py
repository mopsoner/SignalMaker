from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"

DEFAULT_RUNTIME: dict[str, dict[str, Any]] = {
    "executor": {"execution_exchange": "kraken", "quote_assets": "USD,USDC"},
    "kraken": {
        "kraken_exchange_name": "kraken",
        "kraken_base_url": "https://api.kraken.com",
        "kraken_api_key": "",
        "kraken_secret_key": "",
    },
}

LEGACY_ALIASES: dict[tuple[str, str], tuple[str, str]] = {
    ("kraken", "KRAKEN_BASE_URL"): ("kraken", "kraken_base_url"),
    ("kraken", "KRAKEN_API_KEY"): ("kraken", "kraken_api_key"),
    ("kraken", "KRAKEN_SECRET_KEY"): ("kraken", "kraken_secret_key"),
    ("kraken", "EXECUTION_EXCHANGE"): ("executor", "execution_exchange"),
    ("executor", "EXECUTION_EXCHANGE"): ("executor", "execution_exchange"),
    ("executor", "QUOTE_ASSETS"): ("executor", "quote_assets"),
}


def _read_env_file() -> dict[str, str]:
    values: dict[str, str] = {}
    if ENV_PATH.exists():
        for raw in ENV_PATH.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    values.update({key: value for key, value in os.environ.items() if key in {"DATABASE_URL"}})
    return values


def _database_url() -> str:
    values = _read_env_file()
    return values.get("DATABASE_URL", "sqlite:///./signalmaker.db")


def _decode(value: Any) -> Any:
    if value is None:
        return None
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except Exception:
        return value


def _rows_sqlite(database_url: str) -> list[tuple[str, str, Any]]:
    path = database_url.removeprefix("sqlite:///")
    if path.startswith("./"):
        path = str(ROOT / path[2:])
    conn = sqlite3.connect(path)
    try:
        return [(category, key, _decode(value)) for category, key, value in conn.execute("SELECT category, key, value FROM app_settings")]
    finally:
        conn.close()


def _rows_postgres(database_url: str) -> list[tuple[str, str, Any]]:
    import psycopg

    dsn = database_url
    if dsn.startswith("postgresql+psycopg://"):
        dsn = "postgresql://" + dsn[len("postgresql+psycopg://"):]
    if dsn.startswith("postgres+psycopg://"):
        dsn = "postgres://" + dsn[len("postgres+psycopg://"):]
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT category, key, value FROM app_settings")
            return [(category, key, value) for category, key, value in cur.fetchall()]


def _db_rows() -> tuple[list[tuple[str, str, Any]], str | None]:
    database_url = _database_url()
    try:
        if database_url.startswith("sqlite"):
            return _rows_sqlite(database_url), None
        parsed = urlparse(database_url)
        if parsed.scheme.startswith("postgres"):
            return _rows_postgres(database_url), None
        return [], f"unsupported_database_url:{parsed.scheme}"
    except Exception as exc:
        return [], str(exc)


def load_runtime_settings_lightweight() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    payload = {section: values.copy() for section, values in DEFAULT_RUNTIME.items()}
    rows, error = _db_rows()
    seen: set[tuple[str, str]] = set()
    legacy_seen: dict[str, dict[str, Any]] = {}
    for category, key, value in rows:
        original = (category, key)
        target = LEGACY_ALIASES.get(original, original)
        is_alias = target != original
        target_category, target_key = target
        if is_alias:
            legacy_seen.setdefault(category, {})[key] = value
            if target in seen and payload.get(target_category, {}).get(target_key) not in (None, ""):
                continue
        payload.setdefault(target_category, {})[target_key] = value
        if not is_alias:
            seen.add(target)
    return payload, {"database_url_loaded": bool(_database_url()), "db_error": error, "legacy_aliases_seen": legacy_seen}

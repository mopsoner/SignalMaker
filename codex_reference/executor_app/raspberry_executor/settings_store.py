"""Deprecated Raspberry-local settings store.

The runtime source of truth is now the API ``app_settings`` table loaded through
``app.services.runtime_settings``. This module is kept only as a compatibility
adapter so older Raspberry SQLite ``settings`` rows can be migrated into the
canonical app_settings keys before this file is removed.
"""

from typing import Any

from raspberry_executor.sqlite_db import connect, init_db, now_iso


def read_settings() -> dict[str, str]:
    """Read legacy Raspberry SQLite settings for migration/fallback only."""
    init_db()
    with connect() as conn:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        return {str(row["key"]): str(row["value"]) for row in rows}


def write_settings(values: dict[str, Any], *, allowed_keys: set[str] | None = None) -> None:
    """Deprecated compatibility writer for legacy callers.

    New runtime settings must be persisted to app_settings via the API/admin
    runtime settings endpoints, not to this local SQLite table.
    """
    init_db()
    now = now_iso()
    with connect() as conn:
        for key, value in values.items():
            key = str(key)
            if allowed_keys is not None and key not in allowed_keys:
                continue
            conn.execute(
                "INSERT OR REPLACE INTO settings(key, value, updated_at) VALUES(?, ?, ?)",
                (key, str(value), now),
            )


def sync_settings_from_values(values: dict[str, str], *, allowed_keys: set[str] | None = None) -> dict[str, str]:
    """Deprecated: merge values into legacy SQLite settings for old releases."""
    existing = read_settings()
    merged = {**values, **{k: v for k, v in existing.items() if allowed_keys is None or k in allowed_keys}}
    write_settings(merged, allowed_keys=allowed_keys)
    return merged

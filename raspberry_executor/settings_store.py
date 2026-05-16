from typing import Any

from raspberry_executor.sqlite_db import connect, init_db, now_iso


def read_settings() -> dict[str, str]:
    init_db()
    with connect() as conn:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        return {str(row["key"]): str(row["value"]) for row in rows}


def write_settings(values: dict[str, Any], *, allowed_keys: set[str] | None = None) -> None:
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
    existing = read_settings()
    merged = {**values, **{k: v for k, v in existing.items() if allowed_keys is None or k in allowed_keys}}
    write_settings(merged, allowed_keys=allowed_keys)
    return merged

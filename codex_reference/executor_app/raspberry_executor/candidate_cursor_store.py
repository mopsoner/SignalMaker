from datetime import datetime, timezone
from typing import Any

from raspberry_executor.sqlite_db import connect, init_db, now_iso

CURSOR_KEY = "last_remote_candidate_seen_at"
RESET_KEY = "local_runtime_data_reset_at"


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def candidate_time(candidate: dict[str, Any]) -> datetime | None:
    for key in ("created_at", "createdAt", "created", "updated_at", "updatedAt", "timestamp", "time"):
        dt = _parse_dt(candidate.get(key))
        if dt:
            return dt
    return None


def read_candidate_cursor() -> str | None:
    init_db()
    with connect() as conn:
        cursor_row = conn.execute("SELECT value FROM meta WHERE key=?", (CURSOR_KEY,)).fetchone()
        cursor = _parse_dt(cursor_row["value"] if cursor_row else None)
        return cursor.isoformat() if cursor else None


def read_last_runtime_reset_at() -> str | None:
    init_db()
    with connect() as conn:
        reset_row = conn.execute("SELECT value FROM meta WHERE key=?", (RESET_KEY,)).fetchone()
        reset = _parse_dt(reset_row["value"] if reset_row else None)
        return reset.isoformat() if reset else None


def set_candidate_cursor(value: str | None = None) -> str:
    init_db()
    cursor = value or now_iso()
    with connect() as conn:
        conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)", (CURSOR_KEY, cursor))
        conn.commit()
    return cursor


def advance_candidate_cursor(candidates: list[dict[str, Any]]) -> str | None:
    current = _parse_dt(read_candidate_cursor())
    latest = current
    for candidate in candidates:
        dt = candidate_time(candidate)
        if dt and (latest is None or dt > latest):
            latest = dt
    if latest:
        return set_candidate_cursor(latest.isoformat())
    return None


def filter_candidates_after_cursor(candidates: list[dict[str, Any]], cursor: str | None) -> list[dict[str, Any]]:
    cutoff = _parse_dt(cursor)
    if cutoff is None:
        return candidates
    result = []
    for candidate in candidates:
        dt = candidate_time(candidate)
        if dt is None or dt > cutoff:
            result.append(candidate)
    return result

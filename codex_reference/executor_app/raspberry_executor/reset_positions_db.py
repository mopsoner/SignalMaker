from pathlib import Path

from raspberry_executor.sqlite_db import connect, init_db, now_iso

# Admin reset deletes local runtime data only.
# It preserves persistent configuration/settings.
PRESERVED_TABLES = {"settings", "meta", "sqlite_sequence"}
PRESERVED_PREFIXES = {"sqlite_"}
LOCAL_FILES_TO_CLEAR = [
    Path("state.json"),
    Path("raspberry_executor") / "candle_retry_queue.json",
    Path("raspberry_executor") / "pending_trades.json",
]


def _runtime_tables(conn) -> list[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    tables = []
    for row in rows:
        name = str(row["name"])
        if name in PRESERVED_TABLES:
            continue
        if any(name.startswith(prefix) for prefix in PRESERVED_PREFIXES):
            continue
        tables.append(name)
    return tables


def _delete_file(path: Path) -> tuple[bool, str | None]:
    try:
        if path.exists():
            path.unlink()
            return True, None
        return False, None
    except Exception as exc:
        return False, str(exc)


def reset_positions_db() -> dict:
    init_db()
    reset_at = now_iso()
    counts = {}
    errors = {}
    preserved = {}
    deleted_files = {}

    with connect() as conn:
        runtime_tables = _runtime_tables(conn)
        for table in runtime_tables:
            try:
                row = conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
                counts[table] = int(row["count"] if row else 0)
                conn.execute(f"DELETE FROM {table}")
            except Exception as exc:
                counts[table] = None
                errors[table] = str(exc)

        # Store the reset moment so diagnostics can show when the local data was cleared.
        conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES('local_runtime_data_reset_at', ?)", (reset_at,))

        for table in sorted(PRESERVED_TABLES - {"sqlite_sequence"}):
            try:
                row = conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
                preserved[table] = int(row["count"] if row else 0)
            except Exception as exc:
                preserved[table] = None
                errors[f"preserved_{table}"] = str(exc)
        conn.commit()

    for path in LOCAL_FILES_TO_CLEAR:
        deleted, error = _delete_file(path)
        deleted_files[str(path)] = deleted
        if error:
            errors[f"file_{path}"] = error

    return {
        "status": "ok" if not errors else "partial",
        "mode": "delete_runtime_data_only_preserve_settings",
        "reset_at": reset_at,
        "deleted": counts,
        "deleted_files": deleted_files,
        "preserved": preserved,
        "errors": errors,
    }


if __name__ == "__main__":
    print(reset_positions_db())

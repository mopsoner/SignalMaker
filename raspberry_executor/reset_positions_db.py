from pathlib import Path

from raspberry_executor.sqlite_db import connect, init_db

# The reset button must clear every local runtime/tracking table, including any
# future "received" table, while preserving persistent configuration.
PRESERVED_TABLES = {"settings", "meta", "sqlite_sequence"}
PRESERVED_PREFIXES = {"sqlite_"}
LOCAL_FILES_TO_CLEAR = [
    Path("state.json"),
    Path("raspberry_executor") / "candle_retry_queue.json",
    Path("raspberry_executor") / "pending_trades.json",
]


def _all_user_tables(conn) -> list[str]:
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
    counts = {}
    errors = {}
    preserved = {}
    deleted_files = {}

    with connect() as conn:
        for table in _all_user_tables(conn):
            try:
                row = conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
                counts[table] = int(row["count"] if row else 0)
                conn.execute(f"DELETE FROM {table}")
            except Exception as exc:
                counts[table] = None
                errors[table] = str(exc)

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

    try:
        with connect() as conn:
            conn.execute("VACUUM")
    except Exception as exc:
        errors["vacuum"] = str(exc)

    return {
        "status": "ok" if not errors else "partial",
        "deleted": counts,
        "deleted_files": deleted_files,
        "preserved": preserved,
        "errors": errors,
    }


if __name__ == "__main__":
    print(reset_positions_db())

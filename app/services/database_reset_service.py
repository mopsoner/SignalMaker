from pathlib import Path
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from app.models.base import Base
from raspberry_executor.reset_positions_db import reset_positions_db

PRESERVED_TABLES = {"app_settings", "config", "alembic_version"}
PRESERVED_PREFIXES = {"sqlite_"}
ROOT = Path(__file__).resolve().parents[2]
LOG_DIRS = (ROOT / "logs", ROOT / ".runtime")
LOG_SUFFIXES = {".log", ".err", ".out"}


def _table_count(db: Session, table_name: str) -> int | None:
    try:
        row = db.execute(text(f'SELECT COUNT(*) AS count FROM "{table_name}"')).mappings().first()
        return int(row["count"] if row else 0)
    except Exception:
        return None


def _runtime_tables(db: Session) -> list[str]:
    inspector = inspect(db.connection())
    all_tables = set(inspector.get_table_names())
    metadata_order = [table.name for table in reversed(Base.metadata.sorted_tables)]
    ordered = metadata_order + sorted(all_tables - set(metadata_order))
    return [
        name
        for name in ordered
        if name in all_tables
        and name not in PRESERVED_TABLES
        and not any(name.startswith(prefix) for prefix in PRESERVED_PREFIXES)
    ]


def _clear_log_files() -> dict[str, Any]:
    """Truncate local runtime log files as part of the operator reset action."""
    cleared: dict[str, int] = {}
    missing_dirs: list[str] = []
    errors: dict[str, str] = {}

    for directory in LOG_DIRS:
        if not directory.exists():
            missing_dirs.append(str(directory))
            continue
        for path in sorted(directory.iterdir()):
            if not path.is_file() or path.suffix.lower() not in LOG_SUFFIXES:
                continue
            try:
                previous_size = path.stat().st_size
                path.write_text("")
                cleared[str(path)] = previous_size
            except Exception as exc:
                errors[str(path)] = str(exc)

    return {"cleared": cleared, "missing_dirs": missing_dirs, "errors": errors}


def reset_database_preserving_config(db: Session) -> dict[str, Any]:
    """Delete all runtime data while preserving admin/configuration tables.

    The website reset button is also the operator reset for the Raspberry TUI.
    Clear both the application database runtime tables and the local Raspberry
    SQLite runtime tables so old Last Activity / error events do not reappear.
    Settings/config tables are preserved; log files are truncated explicitly.
    """
    deleted: dict[str, int | None] = {}
    errors: dict[str, str] = {}
    preserved: dict[str, int | None] = {}

    for table_name in _runtime_tables(db):
        try:
            deleted[table_name] = _table_count(db, table_name)
            db.execute(text(f'DELETE FROM "{table_name}"'))
        except Exception as exc:
            deleted[table_name] = None
            errors[table_name] = str(exc)

    inspector = inspect(db.connection())
    existing_tables = set(inspector.get_table_names())
    for table_name in sorted(PRESERVED_TABLES & existing_tables):
        preserved[table_name] = _table_count(db, table_name)

    if errors:
        db.rollback()
    else:
        db.commit()

    try:
        raspberry_reset = reset_positions_db()
    except Exception as exc:
        raspberry_reset = {"status": "error", "errors": {"raspberry_executor": str(exc)}}

    log_reset = _clear_log_files()
    all_errors = {
        **errors,
        **{f"raspberry_{k}": v for k, v in (raspberry_reset.get("errors") or {}).items()},
        **{f"log_{k}": v for k, v in log_reset["errors"].items()},
    }

    return {
        "status": "ok" if not all_errors else "error",
        "mode": "delete_all_except_settings_and_clear_logs",
        "deleted": deleted,
        "preserved": preserved,
        "raspberry_executor": raspberry_reset,
        "logs": log_reset,
        "errors": all_errors,
    }

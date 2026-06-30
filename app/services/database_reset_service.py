from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from app.models.base import Base

PRESERVED_TABLES = {"app_settings", "config", "alembic_version"}
PRESERVED_PREFIXES = {"sqlite_"}


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


def reset_database_preserving_config(db: Session) -> dict[str, Any]:
    """Delete all runtime data while preserving admin/configuration tables."""
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

    return {
        "status": "ok" if not errors else "error",
        "mode": "delete_all_except_config",
        "deleted": deleted,
        "preserved": preserved,
        "errors": errors,
    }

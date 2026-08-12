from collections import deque
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.logging import worker_log_candidates
from app.services.notifier_service import NotifierService
from app.services.runtime_settings import (
    delete_runtime_setting_override,
    load_runtime_settings,
    load_runtime_settings_admin,
    persist_runtime_settings,
)
from app.services.worker_control_service import WorkerControlService

router = APIRouter()


class SettingsPayload(BaseModel):
    general: dict[str, Any] = {}
    strategy: dict[str, Any] = {}
    notifications: dict[str, Any] = {}
    bot: dict[str, Any] = {}
    live: dict[str, Any] = {}
    momentum: dict[str, Any] = {}
    stock_etf: dict[str, Any] = {}


@router.get('/admin/settings')
def get_admin_settings(db: Session = Depends(get_db)) -> dict[str, Any]:
    return load_runtime_settings_admin(db)


@router.put('/admin/settings')
def update_admin_settings(payload: SettingsPayload, db: Session = Depends(get_db)) -> dict[str, Any]:
    try:
        persist_runtime_settings(db, payload.model_dump(exclude_unset=True))
        return load_runtime_settings_admin(db)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.delete('/admin/settings/{category}/{key}')
def remove_admin_setting_override(category: str, key: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    return delete_runtime_setting_override(db, category, key)


_PRESERVED_APP_DATA_TABLES = {"app_settings"}


def _quote_identifier(db: Session, identifier: str) -> str:
    return db.bind.dialect.identifier_preparer.quote(identifier)


def _table_row_count(db: Session, table_name: str) -> int:
    quoted = _quote_identifier(db, table_name)
    return int(db.execute(text(f"SELECT COUNT(*) FROM {quoted}")).scalar() or 0)


def _deletable_tables(db: Session) -> list[str]:
    inspector = inspect(db.bind)
    return [
        table_name
        for table_name in inspector.get_table_names()
        if table_name not in _PRESERVED_APP_DATA_TABLES
    ]


@router.delete('/admin/cleanup/app-data')
def clear_application_data(db: Session = Depends(get_db)) -> dict:
    """Clear every database table except operator-managed app settings."""
    tables = _deletable_tables(db)
    details = {table: _table_row_count(db, table) for table in tables}

    if tables:
        dialect = db.bind.dialect.name
        quoted_tables = ", ".join(_quote_identifier(db, table) for table in tables)
        if dialect == "postgresql":
            db.execute(text(f"TRUNCATE TABLE {quoted_tables} RESTART IDENTITY CASCADE"))
        else:
            if dialect == "sqlite":
                db.execute(text("PRAGMA foreign_keys=OFF"))
            for table in tables:
                db.execute(text(f"DELETE FROM {_quote_identifier(db, table)}"))
            if dialect == "sqlite":
                db.execute(text("PRAGMA foreign_keys=ON"))

    db.commit()
    return {
        'deleted': sum(details.values()),
        'details': details,
        'preserved': sorted(_PRESERVED_APP_DATA_TABLES),
    }


@router.get('/admin/workers')
def get_worker_status(db: Session = Depends(get_db)) -> dict:
    return WorkerControlService(db).status()


@router.post('/admin/workers/{worker_name}/start')
def start_worker(worker_name: str) -> dict:
    try:
        return WorkerControlService().start(worker_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/admin/workers/{worker_name}/stop')
def stop_worker(worker_name: str) -> dict:
    try:
        return WorkerControlService().stop(worker_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc



# Keep the log allowlist tied to the same worker registry used by controls and status.
_ALLOWED_LOG_WORKERS = frozenset(WorkerControlService.WORKERS)


@router.get('/admin/logs/{worker_name}')
def get_worker_logs(worker_name: str, lines: int = Query(default=200, ge=1, le=2_000)) -> dict:
    if worker_name not in _ALLOWED_LOG_WORKERS:
        raise HTTPException(status_code=400, detail=f"Unknown worker: {worker_name}")

    existing_logs = [path for path in worker_log_candidates(worker_name) if path.is_file()]
    log_path = max(existing_logs, key=lambda path: path.stat().st_mtime_ns, default=None)
    if log_path is None:
        return {"worker": worker_name, "path": None, "lines": [], "size_bytes": 0}
    try:
        with log_path.open("r", errors="replace") as fh:
            tail = list(deque(fh, maxlen=lines))
        return {
            "worker": worker_name,
            "path": str(log_path),
            "lines": [line.rstrip("\n") for line in tail],
            "size_bytes": log_path.stat().st_size,
        }
    except OSError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post('/admin/test/notifications')
def test_notifications(db: Session = Depends(get_db)) -> dict:
    runtime = load_runtime_settings(db)['notifications']
    return NotifierService().test(
        telegram_chat_id=runtime.get('telegram_chat_id', ''),
        telegram_secret=runtime.get('telegram_secret', ''),
        discord_url=runtime.get('discord_url', ''),
    )

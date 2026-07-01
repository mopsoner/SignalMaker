import os
from collections import deque
from typing import Any

import requests
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.services.database_reset_service import reset_database_preserving_config
from app.services.notifier_service import NotifierService
from app.services.runtime_settings import load_admin_settings, load_runtime_settings, persist_runtime_settings
from app.services.worker_control_service import WorkerControlService
from raspberry_executor.kraken_client import KrakenClient

router = APIRouter()


class SettingsPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    general: dict[str, Any] = {}
    executor: dict[str, Any] = {}
    kraken: dict[str, Any] = {}
    market_data: dict[str, Any] = {}
    strategy: dict[str, Any] = {}
    notifications: dict[str, Any] = {}
    bot: dict[str, Any] = {}
    live: dict[str, Any] = {}
    momentum: dict[str, Any] = {}


@router.get('/admin/settings')
def get_admin_settings(
    include_sources: bool = Query(False), db: Session = Depends(get_db)
) -> dict[str, dict[str, Any]] | dict[str, dict[str, dict[str, Any]]]:
    return load_admin_settings(db, include_sources=include_sources)


@router.put('/admin/settings')
def update_admin_settings(payload: SettingsPayload, db: Session = Depends(get_db)) -> dict[str, dict[str, Any]]:
    persist_runtime_settings(db, payload.model_dump())
    return load_admin_settings(db)


@router.get('/admin/workers')
def get_worker_status() -> dict:
    return WorkerControlService().status()


@router.post('/admin/workers/{worker_name}/start')
def start_worker(worker_name: str) -> dict:
    return WorkerControlService().start(worker_name)


@router.post('/admin/workers/{worker_name}/stop')
def stop_worker(worker_name: str) -> dict:
    return WorkerControlService().stop(worker_name)


@router.post('/admin/reset-database')
def reset_database(db: Session = Depends(get_db)) -> dict:
    return reset_database_preserving_config(db)



@router.post('/admin/test/kraken')
def test_kraken(db: Session = Depends(get_db)) -> dict:
    runtime = load_runtime_settings(db)['kraken']
    base = (runtime.get('kraken_base_url') or 'https://api.kraken.com').rstrip('/')
    client = KrakenClient(
        base,
        str(runtime.get('kraken_api_key') or ''),
        str(runtime.get('kraken_secret_key') or ''),
        dry_run=True,
    )
    credentials_loaded = client.is_configured()
    if not credentials_loaded:
        return {
            'status': 'error',
            'base_url': base,
            'api_key_loaded': bool(client.api_key),
            'secret_key_loaded': bool(client.secret_key),
            'error': 'missing_kraken_api_credentials',
        }
    try:
        account = client.account()
        return {
            'status': 'ok',
            'base_url': base,
            'api_key_loaded': True,
            'secret_key_loaded': True,
            'account_keys': sorted(account.keys())[:20] if isinstance(account, dict) else [],
        }
    except requests.HTTPError as exc:
        response = exc.response
        return {
            'status': 'error',
            'base_url': base,
            'api_key_loaded': True,
            'secret_key_loaded': True,
            'http_status': getattr(response, 'status_code', None),
            'error': response.text[:500] if response is not None else str(exc),
        }
    except Exception as exc:
        return {
            'status': 'error',
            'base_url': base,
            'api_key_loaded': True,
            'secret_key_loaded': True,
            'error': str(exc),
        }


_ALLOWED_WORKERS = {"pipeline", "executor", "scheduler"}
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


@router.get('/admin/logs/{worker_name}')
def get_worker_logs(worker_name: str, lines: int = Query(200, ge=1, le=1000)) -> dict:
    if worker_name not in _ALLOWED_WORKERS:
        raise HTTPException(status_code=400, detail=f"Unknown worker: {worker_name}")
    candidates = [
        os.path.join(_ROOT, "logs", f"{worker_name}.log"),
        os.path.join(_ROOT, ".runtime", f"{worker_name}.log"),
        os.path.join(os.getcwd(), "logs", f"{worker_name}.log"),
        os.path.join(os.getcwd(), ".runtime", f"{worker_name}.log"),
    ]
    log_path = next((p for p in candidates if os.path.isfile(p)), None)
    if log_path is None:
        return {"worker": worker_name, "path": None, "lines": [], "size_bytes": 0}
    try:
        with open(log_path, "r", errors="replace") as fh:
            tail = list(deque(fh, maxlen=lines))
        return {
            "worker": worker_name,
            "path": log_path,
            "lines": [ln.rstrip("\n") for ln in tail],
            "size_bytes": os.path.getsize(log_path),
        }
    except OSError as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post('/admin/test/notifications')
def test_notifications(db: Session = Depends(get_db)) -> dict:
    runtime = load_runtime_settings(db)['notifications']
    return NotifierService().test(
        telegram_chat_id=runtime.get('telegram_chat_id', ''),
        telegram_secret=runtime.get('telegram_secret', ''),
        discord_url=runtime.get('discord_url', ''),
    )

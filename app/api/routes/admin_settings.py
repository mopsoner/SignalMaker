import os
from collections import deque
from typing import Any

import requests
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.services.notifier_service import NotifierService
from app.services.runtime_settings import load_runtime_settings, persist_runtime_settings
from app.services.worker_control_service import WorkerControlService

router = APIRouter()


class SettingsPayload(BaseModel):
    general: dict[str, Any] = {}
    binance: dict[str, Any] = {}
    strategy: dict[str, Any] = {}
    notifications: dict[str, Any] = {}
    bot: dict[str, Any] = {}


@router.get('/admin/settings')
def get_admin_settings(db: Session = Depends(get_db)) -> dict[str, dict[str, Any]]:
    return load_runtime_settings(db)


@router.put('/admin/settings')
def update_admin_settings(payload: SettingsPayload, db: Session = Depends(get_db)) -> dict[str, dict[str, Any]]:
    return persist_runtime_settings(db, payload.model_dump())


@router.get('/admin/workers')
def get_worker_status() -> dict:
    return WorkerControlService().status()


@router.post('/admin/workers/{worker_name}/start')
def start_worker(worker_name: str) -> dict:
    return WorkerControlService().start(worker_name)


@router.post('/admin/workers/{worker_name}/stop')
def stop_worker(worker_name: str) -> dict:
    return WorkerControlService().stop(worker_name)


@router.post('/admin/test/binance')
def test_binance(db: Session = Depends(get_db)) -> dict:
    base = load_runtime_settings(db)['binance']['binance_rest_base'].rstrip('/')
    response = requests.get(f'{base}/api/v3/ping', timeout=10)
    return {'status': 'ok' if response.ok else 'error', 'http_status': response.status_code, 'base_url': base}


_ALLOWED_WORKERS = {"pipeline", "executor", "scheduler"}
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


@router.get('/admin/logs/{worker_name}')
def get_worker_logs(worker_name: str, lines: int = 200) -> dict:
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

import os
from collections import deque

from fastapi import APIRouter, HTTPException

router = APIRouter()

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


@router.get('/admin/workers')
def get_worker_status() -> dict:
    return {"pipeline": {"running": False}, "executor": {"running": False}, "scheduler": {"running": False}}


@router.post('/admin/workers/{worker_name}/start')
def start_worker(worker_name: str) -> dict:
    return {"started": worker_name, "note": "Worker control not available in local dev mode"}


@router.post('/admin/workers/{worker_name}/stop')
def stop_worker(worker_name: str) -> dict:
    return {"stopped": worker_name, "note": "Worker control not available in local dev mode"}

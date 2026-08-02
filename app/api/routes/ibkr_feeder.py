from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

import requests
from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from scripts.ibkr_feeder import asset_config_error, default_status

router = APIRouter()
ROOT = Path(__file__).resolve().parents[3]
_lock = Lock()
_process: subprocess.Popen | None = None

class RunFilters(BaseModel):
    asset_type: str | None = None; region: str | None = None; country: str | None = None
    currency: str | None = None; exchange_code: str | None = None; universe: str | None = None
    pea_eligible: bool | None = None; ucits: bool | None = None
    symbols: list[str] = Field(default_factory=list); provider_symbols: list[str] = Field(default_factory=list)
    max_assets: int | None = None; include_disabled: bool = False

def _paths() -> tuple[Path, Path, Path]:
    return (ROOT / os.getenv("IBKR_FEEDER_STATUS_FILE", "data/ibkr_feeder_status.json"), ROOT / os.getenv("IBKR_FEEDER_ASSETS_FILE", "config/ibkr_assets.json"), ROOT / "data/ibkr_feeder.log")

@router.get("/status")
def status():
    status_path, assets_path, _ = _paths()
    if status_path.exists():
        try: return json.loads(status_path.read_text())
        except (OSError, json.JSONDecodeError): pass
    total = 0
    try:
        data = json.loads(assets_path.read_text()); total = len(data.get("assets", [])) if isinstance(data, dict) else len(data)
    except (OSError, json.JSONDecodeError): pass
    return default_status(total)

@router.post("/check-auth")
def check_auth():
    base = os.getenv("IBKR_CP_BASE_URL", "https://localhost:5000/v1/api").rstrip("/"); checked = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        r = requests.post(base + "/iserver/auth/status", json={}, verify=os.getenv("IBKR_CP_VERIFY_SSL", "false").lower() == "true", timeout=15); r.raise_for_status(); body = r.json(); authenticated = bool(body.get("authenticated") or body.get("iserver", {}).get("authStatus", {}).get("authenticated"))
        return {"ok": authenticated, "reachable": True, "authenticated": authenticated, "base_url": base, "checked_at": checked, "message": None if authenticated else "IBKR Gateway is not authenticated. Open https://localhost:5000 and login first."}
    except Exception as exc: return {"ok": False, "reachable": False, "authenticated": False, "base_url": base, "checked_at": checked, "message": str(exc)}

def _start(filters: RunFilters):
    global _process
    with _lock:
        if _process and _process.poll() is None: return {"ok": False, "started": False, "message": "IBKR feeder is already running"}
        assets_path = _paths()[1]
        if not assets_path.is_file():
            return {"ok": False, "started": False, "message": asset_config_error(assets_path)}
        args = [str(ROOT / ".venv/bin/python") if (ROOT / ".venv/bin/python").exists() else "python3", str(ROOT / "scripts/ibkr_feeder.py")]
        values = filters.model_dump()
        for key in ("asset_type", "region", "country", "currency", "exchange_code", "universe"):
            if values[key]: args += ["--" + key.replace("_", "-"), str(values[key])]
        for key in ("pea_eligible", "ucits"):
            if values[key] is not None: args += ["--" + key.replace("_", "-"), str(values[key]).lower()]
        for symbol in values["symbols"]: args += ["--symbol", symbol]
        for symbol in values["provider_symbols"]: args += ["--provider-symbol", symbol]
        if values["max_assets"]: args += ["--max-assets", str(values["max_assets"])]
        if values["include_disabled"]: args.append("--include-disabled")
        log = _paths()[2]; log.parent.mkdir(parents=True, exist_ok=True); handle = log.open("a", encoding="utf-8")
        _process = subprocess.Popen(args, cwd=ROOT, stdout=handle, stderr=subprocess.STDOUT)
    return {"ok": True, "started": True, "message": "IBKR feeder started", "filters": values}

@router.post("/run-once")
def run_once(filters: RunFilters): return _start(filters)

class AssetRun(BaseModel): symbol: str

@router.post("/run-asset")
def run_asset(body: AssetRun): return _start(RunFilters(symbols=[body.symbol], max_assets=1))

@router.get("/logs")
def logs(lines: int = Query(300, ge=1, le=5000)):
    path = _paths()[2]
    return {"ok": True, "lines": path.read_text(errors="replace").splitlines()[-lines:] if path.exists() else []}

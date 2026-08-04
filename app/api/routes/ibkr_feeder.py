from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

import requests
from fastapi import APIRouter, Query
from pydantic import BaseModel, Field, field_validator

from scripts.ibkr_feeder import CANONICAL_UNIVERSES, canonical_universe, asset_config_error, default_status
from scripts.ibkr_discover_assets import Resolver, discover, read_assets, write_assets

router = APIRouter()
ROOT = Path(__file__).resolve().parents[3]
_lock = Lock()
_processes: dict[str, subprocess.Popen] = {}
_last_discovery: dict | None = None

class RunRequest(BaseModel):
    universe: str

    @field_validator("universe")
    @classmethod
    def validate_universe(cls, value: str) -> str:
        return canonical_universe(value)

def _paths() -> tuple[Path, Path, Path]:
    return (ROOT / os.getenv("IBKR_FEEDER_STATUS_FILE", "data/ibkr_feeder_status.json"), ROOT / os.getenv("IBKR_FEEDER_ASSETS_FILE", "config/ibkr_assets.json"), ROOT / "data/ibkr_feeder.log")

@router.get("/universes")
def universes(): return {"universes": list(CANONICAL_UNIVERSES)}

@router.get("/status")
def status(universe: str | None = None):
    status_path, assets_path, _ = _paths()
    if universe:
        canonical = canonical_universe(universe)
        status_path = ROOT / os.getenv("IBKR_FEEDER_STATUS_FILE", f"data/ibkr_feeder_{canonical.lower().replace(' ', '_')}_status.json")
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

def _start(request: RunRequest):
    universe = canonical_universe(request.universe)
    with _lock:
        process = _processes.get(universe)
        if process and process.poll() is None: return {"ok": False, "started": False, "message": f"A feed is already active for {universe}"}
        assets_path = _paths()[1]
        if not assets_path.is_file():
            return {"ok": False, "started": False, "message": asset_config_error(assets_path)}
        args = [str(ROOT / ".venv/bin/python") if (ROOT / ".venv/bin/python").exists() else "python3", str(ROOT / "scripts/ibkr_feeder.py")]
        args += ["--universe", universe]
        log = _paths()[2]; log.parent.mkdir(parents=True, exist_ok=True); handle = log.open("a", encoding="utf-8")
        _processes[universe] = subprocess.Popen(args, cwd=ROOT, stdout=handle, stderr=subprocess.STDOUT)
    return {"ok": True, "started": True, "message": "Universe feed started", "universe": universe}

@router.post("/run-once")
def run_once(request: RunRequest): return _start(request)

@router.get("/logs")
def logs(lines: int = Query(300, ge=1, le=5000)):
    path = _paths()[2]
    return {"ok": True, "lines": path.read_text(errors="replace").splitlines()[-lines:] if path.exists() else []}

class DiscoveryRequest(BaseModel):
    universe: str; asset_type: str | None = None; source: str = "seed-file"; seed_file: str | None = None
    exchange_code: str | None = None; region: str | None = None; country: str | None = None; currency: str | None = None
    pea_eligible: bool | None = None; ucits: bool | None = None; max_assets: int = Field(50, ge=1, le=1000)
    dry_run: bool = True

@router.get("/assets")
def assets():
    path = _paths()[1]
    values = read_assets(path)
    return {"ok": True, "assets": values, "configured": len(values), "resolved": sum(bool(a.get("conid")) for a in values),
            "without_conid": sum(not bool(a.get("conid")) for a in values), "last_discovery_run": (_last_discovery or {}).get("last_discovery_run")}

@router.post("/discover-assets")
def discover_assets(body: DiscoveryRequest):
    global _last_discovery
    options = body.model_dump(); options["output"] = str(_paths()[1])
    _last_discovery = discover(options)
    return _last_discovery

class SaveAssets(BaseModel): assets: list[dict] | None = None

@router.post("/save-assets")
def save_discovered_assets(body: SaveAssets):
    values = body.assets if body.assets is not None else (_last_discovery or {}).get("assets")
    if values is None: return {"ok": False, "saved": 0, "message": "No discovery result is available to save"}
    write_assets(_paths()[1], values)
    return {"ok": True, "saved": len(values), "configured": len(values)}

class ResolveSymbol(BaseModel):
    symbol: str; asset_type: str | None = None; exchange_code: str | None = None; region: str | None = None
    country: str | None = None; currency: str | None = None; universe: str
    pea_eligible: bool | None = None; ucits: bool | None = None

@router.post("/resolve-symbol")
def resolve_symbol(body: ResolveSymbol):
    resolver = Resolver(); resolver.auth()
    return {"ok": True, "asset": resolver.resolve(body.symbol, body.model_dump())}

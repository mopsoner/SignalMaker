#!/usr/bin/env python3
"""Run a metadata-driven IBKR feed for one SignalMaker universe."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import requests

ROOT = Path(__file__).resolve().parents[1]
CANONICAL_UNIVERSES = ("Europe Stocks", "Europe ETF")
UNIVERSE_ALIASES = {
    "Stocks Euronext Paris": "Europe Stocks", "Stocks Europe": "Europe Stocks",
    "ETF PEA": "Europe ETF", "ETF Europe UCITS": "Europe ETF",
}
# These are workflow requirements, not operator options. IBKR supplies native bars.
COLLECTION_PROFILE = {"shared_workflows": ("15m", "1h", "4h")}
IBKR_BARS = {"15m": "15min", "1h": "1h", "4h": "4h"}
BAR_SECONDS = {"15m": 900, "1h": 3600, "4h": 14400}
ASSET_METADATA = ("symbol", "provider_symbol", "asset_type", "exchange_code", "currency",
                  "region", "country", "isin", "mic", "pea_eligible", "ucits")


def asset_config_error(path: Path) -> str:
    example = ROOT / "config/ibkr_assets.example.json"
    try: target = path.relative_to(ROOT)
    except ValueError: target = path
    try: source = example.relative_to(ROOT)
    except ValueError: source = example
    return f"IBKR asset configuration not found: {path}. Create it with: cp {source} {target}"


def canonical_universe(value: str | None) -> str:
    if not value:
        raise ValueError("--universe is required (Europe Stocks or Europe ETF)")
    canonical = UNIVERSE_ALIASES.get(value, value)
    if canonical not in CANONICAL_UNIVERSES:
        raise ValueError(f"unsupported universe: {value}")
    return canonical


def load_assets(path: Path) -> list[dict]:
    if not path.is_file(): raise FileNotFoundError(asset_config_error(path))
    document = json.loads(path.read_text(encoding="utf-8"))
    assets = document.get("assets", []) if isinstance(document, dict) else document
    if not isinstance(assets, list): raise ValueError(f"IBKR asset configuration must contain an 'assets' list: {path}")
    return assets


def resolve_universe_assets(path: Path, universe: str) -> list[dict]:
    """Resolve enabled assets from the single discovery/configuration source."""
    canonical = canonical_universe(universe)
    result = []
    for source in load_assets(path):
        if not source.get("enabled", True): continue
        try: asset_universe = canonical_universe(source.get("universe"))
        except ValueError: continue
        if asset_universe != canonical: continue
        asset = {key: source.get(key) for key in ASSET_METADATA}
        asset.update({"conid": source.get("conid"), "universe": canonical})
        result.append(asset)
    return result


def filter_assets(assets: list[dict], filters: dict) -> list[dict]:
    """Compatibility helper for discovery views; feed launches no longer expose filters."""
    result = []
    for asset in assets:
        if not filters.get("include_disabled") and not asset.get("enabled", True): continue
        if any(filters.get(k) and str(asset.get(k, "")).casefold() != str(filters[k]).casefold()
               for k in ("asset_type", "region", "country", "currency", "exchange_code", "universe")): continue
        if any(filters.get(k) is not None and bool(asset.get(k)) != filters[k] for k in ("pea_eligible", "ucits")): continue
        if filters.get("symbols") and asset.get("symbol", "").casefold() not in {x.casefold() for x in filters["symbols"]}: continue
        if filters.get("provider_symbols") and asset.get("provider_symbol", "").casefold() not in {x.casefold() for x in filters["provider_symbols"]}: continue
        result.append(asset)
    return result[:filters["max_assets"]] if filters.get("max_assets") else result


def env_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_timestamp(value: Any) -> str:
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.strip().replace(".", "", 1).isdigit()):
        number = float(value) / (1000 if abs(float(value)) >= 100_000_000_000 else 1)
        dt = datetime.fromtimestamp(number, timezone.utc)
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try: dt = datetime.fromisoformat(text)
        except ValueError: dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        dt = (dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt).astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _first(row: dict, *keys: str) -> Any:
    for key in keys:
        if row.get(key) is not None: return row[key]
    raise ValueError(f"missing field ({'/'.join(keys)})")


def parse_ibkr_bars(response: dict[str, Any], timeframe: str | None = None,
                    clock: Callable[[], datetime] | None = None) -> list[dict[str, Any]]:
    rows = next((response[k] for k in ("data", "bars", "candles") if isinstance(response.get(k), list)), [])
    candles = [{"timestamp": normalize_timestamp(_first(row, "t", "time", "date", "timestamp")),
                "open": float(_first(row, "o", "open")), "high": float(_first(row, "h", "high")),
                "low": float(_first(row, "l", "low")), "close": float(_first(row, "c", "close")),
                "volume": float(_first(row, "v", "volume"))} for row in rows]
    if timeframe:
        current = (clock or (lambda: datetime.now(timezone.utc)))()
        candles = [c for c in candles if datetime.fromisoformat(c["timestamp"].replace("Z", "+00:00")) + timedelta(seconds=BAR_SECONDS[timeframe]) <= current]
    return candles


def build_payload(asset: dict, candles: list[dict], timeframe: str = "1d") -> dict:
    # Default remains for import compatibility; production calls always use an internal profile value.
    return {"universe": canonical_universe(asset["universe"]), **{k: asset.get(k) for k in ASSET_METADATA},
            "provider": "IBKR", "timeframe": timeframe, "run_type": "universe_feed",
            "queue_analysis": False, "candles": candles}


def default_status(total: int = 0) -> dict:
    return {"ibkr": {"base_url": os.getenv("IBKR_CP_BASE_URL", "https://localhost:5000/v1/api"), "reachable": False, "authenticated": False},
            "signalmaker": {"base_url": os.getenv("SIGNALMAKER_BASE_URL", "https://mysginalmaker.replit.app"), "ingest_path": os.getenv("SIGNALMAKER_INGEST_PATH", "/api/v1/stocks-etfs/ibkr/candles")},
            "run": {"status": "never_run", "run_id": None, "universe": None, "total_assets": total,
                    "current_asset": None, "current_timeframe": None, "completed_assets": 0,
                    "batches_sent": 0, "candles_collected": 0, "candles_accepted": 0,
                    "errors": [], "retries": 0, "started_at": None, "heartbeat_at": None, "finished_at": None,
                    "internal_timeframes": list(COLLECTION_PROFILE["shared_workflows"])}, "assets": []}


def write_status(path: Path, status: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True); temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(status, indent=2), encoding="utf-8"); temporary.replace(path)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(); p.add_argument("--universe", required=not bool(os.getenv("FEED_UNIVERSE")))
    return p


def _retry(call: Callable[[], requests.Response], status: dict) -> requests.Response:
    attempts = int(os.getenv("IBKR_FEEDER_RETRIES", "3"))
    for attempt in range(attempts):
        try:
            response = call()
            if response.status_code not in {408, 429, 500, 502, 503, 504}: response.raise_for_status(); return response
            response.raise_for_status()
        except requests.RequestException:
            if attempt + 1 == attempts: raise
            status["run"]["retries"] += 1; time.sleep(min(2 ** attempt, float(os.getenv("IBKR_FEEDER_MAX_BACKOFF", "8"))) + random.random() / 10)
    raise RuntimeError("retry exhausted")


def main(argv: list[str] | None = None) -> int:
    try: universe = canonical_universe(parser().parse_args(argv).universe or os.getenv("FEED_UNIVERSE"))
    except SystemExit: return 2
    except ValueError as exc: print(str(exc), file=sys.stderr); return 2
    print(f"Canonical universe: {universe}")
    asset_path = ROOT / os.getenv("IBKR_FEEDER_ASSETS_FILE", "config/ibkr_assets.json")
    status_path = ROOT / os.getenv("IBKR_FEEDER_STATUS_FILE", f"data/ibkr_feeder_{universe.lower().replace(' ', '_')}_status.json")
    lock_path = ROOT / "data" / f"ibkr_feeder_{universe.lower().replace(' ', '_')}.lock"; lock_path.parent.mkdir(exist_ok=True)
    lock = lock_path.open("w")
    try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError: print(f"A feed is already active for {universe}", file=sys.stderr); return 3
    try: assets = resolve_universe_assets(asset_path, universe)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        status = default_status(); status["run"].update(status="configuration_error", universe=universe, finished_at=now())
        write_status(status_path, status); print(f"CONFIGURATION ERROR: {exc}", file=sys.stderr); return 2
    if not assets: print(f"No active assets for {universe}", file=sys.stderr); return 1
    status = default_status(len(assets)); status["run"].update(status="running", run_id=str(uuid.uuid4()), universe=universe, started_at=now(), heartbeat_at=now()); write_status(status_path, status)
    base = status["ibkr"]["base_url"].rstrip("/"); sm = status["signalmaker"]["base_url"].rstrip("/"); verify = env_bool("IBKR_CP_VERIFY_SSL")
    session = requests.Session(); headers = {"x-operator-key": os.environ["SIGNALMAKER_OPERATOR_KEY"]} if os.getenv("SIGNALMAKER_OPERATOR_KEY") else {}
    checkpoints_path = ROOT / os.getenv("IBKR_FEEDER_CHECKPOINT_FILE", "data/ibkr_feeder_checkpoints.json")
    try: checkpoints = json.loads(checkpoints_path.read_text())
    except (OSError, json.JSONDecodeError): checkpoints = {}
    try:
        auth = session.post(base + "/iserver/auth/status", json={}, verify=verify, timeout=15); auth.raise_for_status()
        body = auth.json(); authenticated = bool(body.get("authenticated") or body.get("iserver", {}).get("authStatus", {}).get("authenticated")); status["ibkr"].update(reachable=True, authenticated=authenticated)
        if not authenticated: raise RuntimeError("IBKR gateway is not authenticated")
        for asset in assets:
            status["run"]["current_asset"] = asset["symbol"]; item = {**asset, "status": "running", "errors": []}
            for timeframe in COLLECTION_PROFILE["shared_workflows"]:
                status["run"].update(current_timeframe=timeframe, heartbeat_at=now()); write_status(status_path, status)
                key = "|".join((universe, str(asset["provider_symbol"]), timeframe)); params = {"conid": asset["conid"], "period": os.getenv("IBKR_FEEDER_PERIOD", "1y"), "bar": IBKR_BARS[timeframe], "outsideRth": "false"}
                if checkpoints.get(key): params["startTime"] = normalize_timestamp(datetime.fromisoformat(checkpoints[key].replace("Z", "+00:00")) - timedelta(seconds=2 * BAR_SECONDS[timeframe]))
                try:
                    fetched = _retry(lambda: session.get(base + "/iserver/marketdata/history", params=params, verify=verify, timeout=60), status)
                    candles = parse_ibkr_bars(fetched.json(), timeframe); status["run"]["candles_collected"] += len(candles)
                    posted = _retry(lambda: session.post(sm + status["signalmaker"]["ingest_path"], json=build_payload(asset, candles, timeframe), headers=headers, timeout=60), status)
                    answer = posted.json(); accepted = int(answer.get("accepted", answer.get("upserted", answer.get("candles_upserted", len(candles)))))
                    status["run"]["batches_sent"] += 1; status["run"]["candles_accepted"] += accepted
                    if candles: checkpoints[key] = candles[-1]["timestamp"]; checkpoints_path.parent.mkdir(exist_ok=True); write_status(checkpoints_path, checkpoints)
                except Exception as exc:
                    failure = {"asset": asset["symbol"], "timeframe": timeframe, "error": str(exc)}; item["errors"].append(failure); status["run"]["errors"].append(failure)
            item["status"] = "completed_with_errors" if item["errors"] else "completed"; status["assets"].append(item); status["run"]["completed_assets"] += 1
        status["run"].update(current_asset=None, current_timeframe=None, heartbeat_at=now())
        # One analysis request per coherent universe run; never send timeframes.
        if not status["run"]["errors"]:
            analysis_path = os.getenv("SIGNALMAKER_ANALYSIS_PATH", "/api/v1/stocks-etfs/analysis/queue")
            _retry(lambda: session.post(sm + analysis_path, json={"market_scope": "stock_etf", "universe": universe}, headers=headers, timeout=60), status)
        status["run"].update(status="completed" if not status["run"]["errors"] else "completed_with_errors", finished_at=now(), heartbeat_at=now()); write_status(status_path, status)
        return 0
    except Exception as exc:
        status["run"]["errors"].append({"error": str(exc)}); status["run"].update(status="failed", finished_at=now(), heartbeat_at=now()); write_status(status_path, status); print(str(exc), file=sys.stderr); return 1


if __name__ == "__main__": sys.exit(main())

#!/usr/bin/env python3
"""Fetch IBKR Client Portal history and forward normalized bars to SignalMaker."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]

def env_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}

def now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def normalize_timestamp(value: Any) -> str:
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.strip().replace(".", "", 1).isdigit()):
        number = float(value)
        if abs(number) >= 100_000_000_000:
            number /= 1000
        dt = datetime.fromtimestamp(number, timezone.utc)
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _first(row: dict, *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    raise ValueError(f"missing field ({'/'.join(keys)})")

def parse_ibkr_bars(response: dict[str, Any]) -> list[dict[str, Any]]:
    rows = next((response[k] for k in ("data", "bars", "candles") if isinstance(response.get(k), list)), [])
    candles = []
    for row in rows:
        candles.append({
            "timestamp": normalize_timestamp(_first(row, "t", "time", "date", "timestamp")),
            "open": float(_first(row, "o", "open")), "high": float(_first(row, "h", "high")),
            "low": float(_first(row, "l", "low")), "close": float(_first(row, "c", "close")),
            "volume": float(_first(row, "v", "volume")),
        })
    return candles

def filter_assets(assets: list[dict], filters: dict) -> list[dict]:
    result = []
    scalar = ("asset_type", "region", "country", "currency", "exchange_code", "universe")
    for asset in assets:
        if not filters.get("include_disabled") and not asset.get("enabled", True): continue
        if any(filters.get(k) and str(asset.get(k, "")).casefold() != str(filters[k]).casefold() for k in scalar): continue
        if any(filters.get(k) is not None and bool(asset.get(k)) != filters[k] for k in ("pea_eligible", "ucits")): continue
        if filters.get("symbols") and asset.get("symbol", "").casefold() not in {x.casefold() for x in filters["symbols"]}: continue
        if filters.get("provider_symbols") and asset.get("provider_symbol", "").casefold() not in {x.casefold() for x in filters["provider_symbols"]}: continue
        result.append(asset)
    return result[:filters["max_assets"]] if filters.get("max_assets") else result

def build_payload(asset: dict, candles: list[dict], timeframe: str = "1d") -> dict:
    keys = ("provider_symbol", "symbol", "asset_type", "currency", "exchange_code", "universe", "name", "region", "country", "isin", "mic", "pea_eligible", "ucits", "priority")
    return {"provider": "IBKR", **{k: asset.get(k) for k in keys}, "timeframe": timeframe,
            "run_type": "local_ibkr_feeder", "queue_analysis": False, "candles": candles}

def default_status(total: int = 0) -> dict:
    return {"ibkr": {"base_url": os.getenv("IBKR_CP_BASE_URL", "https://localhost:5000/v1/api"), "reachable": False, "authenticated": False, "last_auth_check_at": None, "last_error": None},
            "signalmaker": {"base_url": os.getenv("SIGNALMAKER_BASE_URL", "https://mysginalmaker.replit.app"), "ingest_path": os.getenv("SIGNALMAKER_INGEST_PATH", "/api/v1/stocks-etfs/ibkr/candles"), "reachable": False, "last_successful_post_at": None, "last_failed_post_at": None, "last_error": None},
            "run": {"status": "never_run", "started_at": None, "finished_at": None, "duration_seconds": None, "total_assets": total, "selected_assets": 0, "processed": 0, "success_count": 0, "failed_count": 0, "candles_received": 0, "candles_posted": 0}, "filters": {"configured_assets": total, "selected_assets": 0}, "assets": []}

def write_status(path: Path, status: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(status, indent=2), encoding="utf-8")
    temporary.replace(path)

def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    for flag in ("asset-type", "region", "country", "currency", "exchange-code", "universe"): p.add_argument("--" + flag)
    p.add_argument("--pea-eligible", type=lambda x: x.lower() == "true"); p.add_argument("--ucits", type=lambda x: x.lower() == "true")
    p.add_argument("--symbol", action="append", default=[]); p.add_argument("--provider-symbol", action="append", default=[])
    p.add_argument("--max-assets", type=int); p.add_argument("--include-disabled", action="store_true")
    return p

def get_filters(args: argparse.Namespace) -> dict:
    result = vars(args).copy()
    mapping = {"asset_type":"ASSET_TYPE", "region":"REGION", "country":"COUNTRY", "currency":"CURRENCY", "exchange_code":"EXCHANGE_CODE", "universe":"UNIVERSE"}
    for key, suffix in mapping.items(): result[key] = result[key] or os.getenv("IBKR_FEEDER_FILTER_" + suffix) or None
    for key in ("pea_eligible", "ucits"):
        value = os.getenv("IBKR_FEEDER_FILTER_" + key.upper())
        if result[key] is None and value: result[key] = value.lower() == "true"
    result["symbols"] = result.pop("symbol") or [x.strip() for x in os.getenv("IBKR_FEEDER_FILTER_SYMBOLS", "").split(",") if x.strip()]
    result["provider_symbols"] = result.pop("provider_symbol")
    result["max_assets"] = result["max_assets"] or (int(os.environ["IBKR_FEEDER_MAX_ASSETS"]) if os.getenv("IBKR_FEEDER_MAX_ASSETS") else None)
    return result

def main(argv: list[str] | None = None) -> int:
    filters = get_filters(parser().parse_args(argv)); asset_path = ROOT / os.getenv("IBKR_FEEDER_ASSETS_FILE", "config/ibkr_assets.json")
    assets_doc = json.loads(asset_path.read_text()); assets = assets_doc.get("assets", []) if isinstance(assets_doc, dict) else assets_doc
    selected = filter_assets(assets, filters); print(f"Selected {len(selected)} assets from {len(assets)} configured assets.")
    if not selected: print("No IBKR assets matched the selected filters."); return 1
    status_path = ROOT / os.getenv("IBKR_FEEDER_STATUS_FILE", "data/ibkr_feeder_status.json"); status = default_status(len(assets)); started = time.monotonic()
    status["filters"] = {**filters, "configured_assets": len(assets), "selected_assets": len(selected)}; status["run"].update(status="running", started_at=now(), selected_assets=len(selected)); write_status(status_path, status)
    base = status["ibkr"]["base_url"].rstrip("/"); verify = env_bool("IBKR_CP_VERIFY_SSL")
    try:
        response = requests.post(base + "/iserver/auth/status", json={}, verify=verify, timeout=15); response.raise_for_status(); auth = response.json(); authenticated = bool(auth.get("authenticated") or auth.get("iserver", {}).get("authStatus", {}).get("authenticated"))
        status["ibkr"].update(reachable=True, authenticated=authenticated, last_auth_check_at=now())
    except Exception as exc: status["ibkr"].update(last_auth_check_at=now(), last_error=str(exc)); authenticated = False
    if not authenticated:
        status["run"].update(status="failed", finished_at=now()); write_status(status_path, status); print("IBKR gateway is not authenticated. Open https://localhost:5000 and login first."); return 1
    session = requests.Session(); ingest = status["signalmaker"]["base_url"].rstrip("/") + status["signalmaker"]["ingest_path"]; headers = {}
    if os.getenv("SIGNALMAKER_OPERATOR_KEY"): headers["x-operator-key"] = os.environ["SIGNALMAKER_OPERATOR_KEY"]
    for asset in selected:
        item = {k: asset.get(k) for k in ("symbol", "provider_symbol", "conid", "asset_type", "currency", "exchange_code", "region", "country", "universe")}; item.update(last_fetch_status="ERROR", last_fetch_at=now(), candles_received=0, candles_posted=0, last_error=None)
        try:
            r = session.get(base + "/iserver/marketdata/history", params={"conid":asset["conid"], "period":os.getenv("IBKR_FEEDER_PERIOD","1y"), "bar":os.getenv("IBKR_FEEDER_BAR","1d"), "outsideRth":str(env_bool("IBKR_FEEDER_OUTSIDE_RTH", True)).lower()}, verify=verify, timeout=60); r.raise_for_status(); candles = parse_ibkr_bars(r.json()); item["candles_received"] = len(candles)
            posted = session.post(ingest, json=build_payload(asset, candles, os.getenv("IBKR_FEEDER_TIMEFRAME","1d")), headers=headers, timeout=60); posted.raise_for_status(); answer = posted.json(); count = int(answer.get("upserted", answer.get("candles_upserted", len(candles)))); item.update(last_fetch_status="SUCCESS", candles_posted=count); status["signalmaker"].update(reachable=True, last_successful_post_at=now(), last_error=None); status["run"]["success_count"] += 1; print(f"OK {asset['symbol']} received={len(candles)} upserted={count} asset_created={str(answer.get('asset_created', False)).lower()}")
        except Exception as exc:
            item["last_error"] = str(exc); status["run"]["failed_count"] += 1; status["signalmaker"].update(last_failed_post_at=now(), last_error=str(exc)); print(f"ERROR {asset.get('symbol')} step=fetch_or_post message={exc}")
        status["assets"].append(item); status["run"]["processed"] += 1; status["run"]["candles_received"] += item["candles_received"]; status["run"]["candles_posted"] += item["candles_posted"]; write_status(status_path, status); time.sleep(float(os.getenv("IBKR_FEEDER_SLEEP_SECONDS", "1.0")))
    status["run"].update(status="idle" if status["run"]["success_count"] else "failed", finished_at=now(), duration_seconds=round(time.monotonic()-started, 2)); write_status(status_path, status)
    return 0 if status["run"]["success_count"] else 1

if __name__ == "__main__": sys.exit(main())

#!/usr/bin/env python3
"""Discover symbols and resolve them to IBKR Client Portal contracts."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
SEEDS = {
    "Stocks US": "us_stocks.txt", "Stocks Europe": "euronext_paris_stocks.txt",
    "Stocks Euronext Paris": "euronext_paris_stocks.txt", "ETF Europe UCITS": "etf_europe_ucits.txt",
    "ETF PEA": "etf_pea.txt",
}
EXCHANGES = {
    "PA": {"PA", "XPAR", "SBF"}, "US": {"SMART", "NASDAQ", "NYSE", "ARCA", "AMEX"},
    "AMS": {"AMS", "XAMS"}, "XETRA": {"XETRA", "IBIS", "XETR"}, "LSE": {"LSE", "XLON"},
}

def utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def boolean(value: str) -> bool:
    if value.lower() not in {"true", "false"}: raise argparse.ArgumentTypeError("expected true or false")
    return value.lower() == "true"

def read_assets(path: Path) -> list[dict]:
    if not path.exists(): return []
    value = json.loads(path.read_text(encoding="utf-8"))
    return value.get("assets", []) if isinstance(value, dict) else value

def write_assets(path: Path, assets: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(assets, key=lambda a: (str(a.get("universe", "")), str(a.get("asset_type", "")), -int(a.get("priority", 0)), str(a.get("symbol", ""))))
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps({"assets": ordered}, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)

def raw_symbol(symbol: str) -> str:
    return symbol.strip().upper().split(".", 1)[0]

def _contracts(payload: Any) -> list[dict]:
    """Flatten both legacy search rows and rows containing contracts sections."""
    rows = payload if isinstance(payload, list) else payload.get("data", payload.get("contracts", [])) if isinstance(payload, dict) else []
    result = []
    for row in rows:
        nested = row.get("contracts") if isinstance(row, dict) else None
        if isinstance(nested, list):
            for contract in nested: result.append({**row, **contract, "contracts": None})
        elif isinstance(row, dict): result.append(row)
    return result

def _value(row: dict, *keys: str) -> Any:
    for key in keys:
        if row.get(key) not in (None, ""): return row[key]
    return None

def score_contract(row: dict, symbol: str, asset_type: str, exchange_code: str | None, currency: str | None) -> int:
    sec = str(_value(row, "secType", "sec_type", "assetClass") or "").upper()
    if asset_type == "STOCK" and sec and sec != "STK": return -1000
    if asset_type == "ETF" and sec and sec not in {"ETF", "STK"}: return -1000
    score = 100 if str(row.get("symbol", "")).upper() == raw_symbol(symbol) else 0
    exchange = str(_value(row, "listingExchange", "exchange", "primaryExchange", "primary_exchange") or "").upper()
    primary = str(_value(row, "primaryExchange", "primary_exchange") or "").upper()
    accepted = EXCHANGES.get((exchange_code or "").upper(), {(exchange_code or "").upper()})
    if exchange in accepted or primary in accepted or (exchange == "SMART" and primary in accepted): score += 40
    if currency and str(row.get("currency", "")).upper() == currency.upper(): score += 20
    if (asset_type == "STOCK" and sec == "STK") or (asset_type == "ETF" and sec in {"ETF", "STK"}): score += 10
    return score

class Resolver:
    def __init__(self, base_url: str | None = None, verify: bool | None = None, session=None):
        self.base = (base_url or os.getenv("IBKR_CP_BASE_URL", "https://localhost:5000/v1/api")).rstrip("/")
        self.verify = (os.getenv("IBKR_CP_VERIFY_SSL", "false").lower() == "true") if verify is None else verify
        self.session = session or requests.Session()

    def auth(self) -> None:
        response = self.session.post(self.base + "/iserver/auth/status", json={}, verify=self.verify, timeout=15); response.raise_for_status()
        body = response.json()
        if not (body.get("authenticated") or body.get("iserver", {}).get("authStatus", {}).get("authenticated")):
            raise RuntimeError("IBKR Gateway is not authenticated. Open https://localhost:5000 and login first.")

    def search(self, symbol: str) -> list[dict]:
        response = self.session.get(self.base + "/iserver/secdef/search", params={"symbol": raw_symbol(symbol)}, verify=self.verify, timeout=30)
        if response.status_code in {404, 405}:
            response = self.session.post(self.base + "/iserver/secdef/search", json={"symbol": raw_symbol(symbol)}, verify=self.verify, timeout=30)
        response.raise_for_status(); return _contracts(response.json())

    def resolve(self, symbol: str, options: dict) -> dict:
        candidates = self.search(symbol)
        ranked = sorted(candidates, key=lambda row: score_contract(row, symbol, options["asset_type"], options.get("exchange_code"), options.get("currency")), reverse=True)
        if not ranked or score_contract(ranked[0], symbol, options["asset_type"], options.get("exchange_code"), options.get("currency")) < 0:
            raise LookupError("no matching IBKR contract")
        row = ranked[0]; conid = _value(row, "conid", "conidEx", "contractId")
        if conid is None: raise LookupError("matching contract has no conid")
        exchange = _value(row, "listingExchange", "exchange", "primaryExchange")
        return {"enabled": True, "symbol": symbol, "provider_symbol": symbol, "conid": str(conid).split("@")[0],
                "asset_type": options["asset_type"], "currency": _value(row, "currency") or options.get("currency"),
                "exchange_code": options.get("exchange_code"), "region": options.get("region"), "country": options.get("country"),
                "universe": options["universe"], "name": _value(row, "description", "companyName", "name") or raw_symbol(symbol),
                "isin": _value(row, "isin"), "mic": _value(row, "listingExchange", "mic"),
                "pea_eligible": bool(options.get("pea_eligible")), "ucits": bool(options.get("ucits")), "priority": 100,
                "ibkr": {"resolved_at": utcnow(), "raw_symbol": raw_symbol(symbol), "selected_exchange": exchange,
                         "primary_exchange": _value(row, "primaryExchange", "primary_exchange"), "sec_type": _value(row, "secType", "sec_type")}}

def seed_path(options: dict) -> Path:
    requested = options.get("seed_file") or str(ROOT / "config/asset_universe_seeds" / SEEDS[options["universe"]])
    path = Path(requested); return path if path.is_absolute() else ROOT / path

def discover(options: dict, resolver: Resolver | None = None, authenticate: bool = True) -> dict:
    output = Path(options.get("output") or ROOT / "config/ibkr_assets.json"); output = output if output.is_absolute() else ROOT / output
    existing = read_assets(output); by_symbol = {a.get("provider_symbol", "").upper(): a for a in existing}
    resolver = resolver or Resolver()
    if authenticate: resolver.auth()
    source = options.get("source", "seed-file"); notices = []
    # Client Portal scanner availability varies. Probe it, then use deterministic universe seeds as fallback.
    if source == "ibkr-scanner":
        try:
            response = resolver.session.get(resolver.base + "/iserver/scanner/params", verify=resolver.verify, timeout=15); response.raise_for_status()
            notices.append("IBKR scanner parameters available; seed symbols are used for deterministic contract resolution.")
        except Exception as exc: notices.append(f"IBKR scanner unavailable ({exc}); falling back to seed-file mode.")
    path = seed_path(options)
    if not path.is_file(): raise FileNotFoundError(f"seed file not found: {path}")
    symbols = [line.strip() for line in path.read_text().splitlines() if line.strip() and not line.lstrip().startswith("#")][:options.get("max_assets", 50)]
    assets, errors = [], []
    for symbol in symbols:
        old = by_symbol.get(symbol.upper())
        if old and old.get("conid") and not options.get("refresh"):
            assets.append(old); continue
        try:
            asset = resolver.resolve(symbol, options)
            if old and old.get("enabled") is False: asset["enabled"] = False
            assets.append(asset)
        except Exception as exc: errors.append({"input_symbol": symbol, "provider_symbol": symbol, "status": "failed", "error": str(exc)})
    merged = dict(by_symbol)
    for asset in assets: merged[asset["provider_symbol"].upper()] = asset
    final = list(merged.values()) if options.get("append", True) else assets
    if not options.get("dry_run"): write_assets(output, final)
    return {"ok": True, "configured_assets_before": len(existing), "resolved": sum(bool(a.get("conid")) for a in assets),
            "failed": len(errors), "dry_run": bool(options.get("dry_run")), "assets": assets, "errors": errors,
            "notices": notices, "last_discovery_run": utcnow(), "output": str(output)}

def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__); p.add_argument("--source", choices=["seed-file", "ibkr-search", "ibkr-scanner"], default="seed-file")
    p.add_argument("--seed-file"); p.add_argument("--universe", required=True, choices=list(SEEDS)); p.add_argument("--asset-type", required=True, choices=["STOCK", "ETF"])
    for name in ("exchange-code", "region", "country", "currency"): p.add_argument("--" + name)
    p.add_argument("--pea-eligible", type=boolean); p.add_argument("--ucits", type=boolean); p.add_argument("--output", default="config/ibkr_assets.json")
    p.add_argument("--append", action="store_true", default=True); p.add_argument("--refresh", action="store_true"); p.add_argument("--dry-run", action="store_true"); p.add_argument("--max-assets", type=int, default=50)
    return p

def main(argv: list[str] | None = None) -> int:
    try:
        result = discover(vars(parser().parse_args(argv))); print(json.dumps(result, indent=2)); return 0 if not result["failed"] else 1
    except Exception as exc: print(f"Asset discovery failed: {exc}", file=sys.stderr); return 2

if __name__ == "__main__": sys.exit(main())

from __future__ import annotations

import json, time
from pathlib import Path
from typing import Any

from raspberry_executor.config import load_settings
from raspberry_executor.feed_run_store import record_feed_run
from raspberry_executor.ibkr_client_portal import IBKRClientPortal
from raspberry_executor.ibkr_contract_store import get_cached_contract, load_cache, mark_error, upsert_cached_contract
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.signalmaker_client import SignalMakerClient

logger = setup_logging("ibkr-market-feed")


def ibkr_symbol(provider_symbol: str) -> str:
    return provider_symbol.split(".", 1)[0].upper()


def _load_json(path: str, default):
    try:
        return json.loads(Path(path).read_text())
    except Exception:
        return default


def _save_json(path: str, value) -> None:
    p = Path(path); p.parent.mkdir(parents=True, exist_ok=True); p.write_text(json.dumps(value, indent=2, sort_keys=True))


def resolve_contract(client: IBKRClientPortal, settings, asset: dict[str, Any]) -> dict[str, Any]:
    provider_symbol = str(asset.get("provider_symbol") or asset.get("symbol") or "").upper()
    cached = get_cached_contract(settings.ibkr_contract_cache_path, provider_symbol)
    if cached and cached.get("conid") and not cached.get("ambiguous"):
        return cached
    symbol = ibkr_symbol(provider_symbol)
    sec_type = "STK" if str(asset.get("asset_type") or "").upper() == "STOCK" else None
    candidates = client.search_contracts(symbol, sec_type=sec_type)
    currency = str(asset.get("currency") or "EUR").upper()
    strong = [c for c in candidates if str(c.get("symbol") or "").upper() == symbol]
    cur = [c for c in strong if str(c.get("currency") or "").upper() == currency]
    chosen_pool = cur or strong or candidates
    if not chosen_pool:
        raise RuntimeError("no_ibkr_contract_candidates")
    ambiguous = len(chosen_pool) > 1
    chosen = chosen_pool[0]
    conid = chosen.get("conid") or chosen.get("conidEx")
    payload = {
        "ibkr_symbol": symbol,
        "conid": int(str(conid).split(";", 1)[0]),
        "asset_type": str(asset.get("asset_type") or "").upper(),
        "currency": chosen.get("currency") or asset.get("currency"),
        "exchange": chosen.get("exchange") or chosen.get("listingExchange") or "SMART",
        "ambiguous": ambiguous,
        "raw": chosen,
    }
    return upsert_cached_contract(settings.ibkr_contract_cache_path, provider_symbol, payload)


def load_assets(sm: SignalMakerClient, settings, symbols: list[str] | None = None) -> list[dict[str, Any]]:
    syms = symbols or settings.ibkr_market_feed_symbols
    if syms:
        return [{"provider_symbol": s.upper(), "symbol": ibkr_symbol(s), "asset_type": "ETF" if not s.endswith(".US") else "STOCK", "enabled": True} for s in syms]
    assets = sm.list_stock_etf_assets(limit=settings.ibkr_market_feed_limit)
    universes = set(settings.ibkr_market_feed_universes)
    types = set(settings.ibkr_market_feed_asset_types)
    return [a for a in assets if a.get("enabled", True) and (not universes or a.get("universe") in universes) and str(a.get("asset_type") or "").upper() in types]


def run_once(symbols: list[str] | None = None, limit: int | None = None) -> dict[str, Any]:
    settings = load_settings()
    sm = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    cp = IBKRClientPortal(settings)
    retry = _load_json(settings.ibkr_market_feed_retry_queue_path, [])
    summary = {"status": "ok", "symbol_count": 0, "pushed": [], "skipped": [], "errors": [], "retry_queue_size": len(retry)}
    try:
        cp.ensure_ready()
    except Exception as exc:
        summary.update({"status": "blocked", "reason": "ibkr_cp_not_authenticated", "errors": [str(exc)]})
        record_feed_run(summary); return summary
    probe = sm.check_stock_etf_ibkr_ingest_endpoint()
    if not probe.get("ok"):
        summary.update({"status": "blocked", "reason": "signalmaker_ingest_unreachable", "errors": [probe]})
        record_feed_run(summary); return summary
    assets = load_assets(sm, settings, symbols=symbols)[: limit or settings.ibkr_market_feed_limit]
    summary["symbol_count"] = len(assets)
    delay = 60.0 / max(1, settings.ibkr_market_feed_requests_per_minute)
    for asset in assets:
        provider_symbol = str(asset.get("provider_symbol") or asset.get("symbol") or "").upper()
        try:
            contract = resolve_contract(cp, settings, asset)
            if contract.get("ambiguous"):
                summary["skipped"].append({"symbol": provider_symbol, "reason": "ambiguous_contract"}); continue
            candles = cp.historical_bars(contract["conid"], period=settings.ibkr_market_feed_period, bar=settings.ibkr_market_feed_bar, source=settings.ibkr_market_feed_source, outside_rth=settings.ibkr_market_feed_outside_rth, exchange=contract.get("exchange"))
            response = sm.post_stock_etf_ibkr_candles({**asset, **contract}, candles, timeframe=settings.ibkr_market_feed_bar, queue_analysis=settings.ibkr_market_feed_queue_analysis)
            summary["pushed"].append({"symbol": provider_symbol, "candles": len(candles), "response": response})
        except Exception as exc:
            mark_error(settings.ibkr_contract_cache_path, provider_symbol, str(exc))
            retry.append({"symbol": provider_symbol, "error": str(exc), "at": time.time()})
            summary["errors"].append({"symbol": provider_symbol, "error": str(exc)})
        time.sleep(delay)
    _save_json(settings.ibkr_market_feed_retry_queue_path, retry[-1000:])
    summary["retry_queue_size"] = len(retry[-1000:])
    if summary["errors"] and not summary["pushed"]:
        summary["status"] = "error"
    record_feed_run(summary)
    return summary


def status() -> dict[str, Any]:
    settings = load_settings(); cache = load_cache(settings.ibkr_contract_cache_path); retry = _load_json(settings.ibkr_market_feed_retry_queue_path, [])
    return {"enabled": settings.ibkr_market_feed_enabled, "cp_base_url": settings.ibkr_cp_base_url.replace("localhost", "local-gateway"), "retry_queue_size": len(retry), "contract_cache_count": len(cache), "ambiguous_contracts_count": len([v for v in cache.values() if isinstance(v, dict) and v.get("ambiguous")]), "universes": settings.ibkr_market_feed_universes, "asset_types": settings.ibkr_market_feed_asset_types, "poll_seconds": settings.ibkr_market_feed_poll_seconds, "period": settings.ibkr_market_feed_period, "bar": settings.ibkr_market_feed_bar, "source": settings.ibkr_market_feed_source}


def run_loop() -> None:
    while True:
        settings = load_settings()
        if not settings.ibkr_market_feed_enabled:
            logger.info("IBKR market feed disabled")
            time.sleep(settings.ibkr_market_feed_poll_seconds); continue
        try: logger.info("IBKR market feed summary=%s", run_once())
        except Exception as exc: logger.exception("IBKR market feed loop error=%s", exc)
        time.sleep(settings.ibkr_market_feed_poll_seconds)

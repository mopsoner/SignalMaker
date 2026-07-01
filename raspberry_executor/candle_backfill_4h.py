"""4H history backfill sender.

Runs on the Raspberry executor. It fetches older Kraken/Kraken 4h candles and sends
those candles to remote SignalMaker through POST /api/v1/market-data/candles.
Main does not fetch any missing candles itself.
"""

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from raspberry_executor.candle_auto_feed import RateLimiter, effective_kraken_requests_per_minute, resolve_feed_symbols
from raspberry_executor.candle_push_once import fetch_exchange_klines
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ROOT, ensure_env, read_env
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.signalmaker_client import SignalMakerClient

logger = setup_logging("raspberry-candle-backfill-4h")
STATE_PATH = ROOT / "raspberry_executor" / "candle_backfill_4h_state.json"
INTERVAL = "4h"
FOUR_HOURS_MS = 4 * 60 * 60 * 1000
DEFAULT_DAYS = 365


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"symbols": {}, "runs": []}
    try:
        data = json.loads(STATE_PATH.read_text())
        return data if isinstance(data, dict) else {"symbols": {}, "runs": []}
    except Exception:
        return {"symbols": {}, "runs": []}


def _save_state(state: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True))


def _utc_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _start_ms_for_days(days: int) -> int:
    return _utc_ms(datetime.now(timezone.utc) - timedelta(days=max(1, int(days))))


def _latest_main_close_time(client: SignalMakerClient, symbol: str, interval: str = INTERVAL) -> int | None:
    try:
        latest = client.latest_candle(symbol, interval)
        if latest and latest.get("close_time") is not None:
            return int(latest["close_time"])
    except Exception as exc:
        logger.warning("remote latest candle lookup failed symbol=%s interval=%s error=%s", symbol, interval, str(exc))
    return None


def backfill_symbol(settings, client: SignalMakerClient, limiter: RateLimiter, state: dict[str, Any], symbol: str, *, days: int, chunk_limit: int, max_chunks: int, post_sleep: float) -> dict[str, Any]:
    symbol = symbol.upper()
    symbols_state = state.setdefault("symbols", {})
    symbol_state = symbols_state.setdefault(symbol, {})
    from_ms = _start_ms_for_days(days)
    now_ms = _utc_ms(datetime.now(timezone.utc))
    latest_main = _latest_main_close_time(client, symbol)
    cursor = max(from_ms, int(symbol_state.get("next_start_time") or 0), int(latest_main + 1) if latest_main is not None and latest_main >= from_ms else from_ms)
    if cursor >= now_ms - FOUR_HOURS_MS:
        symbol_state.update({"status": "complete", "last_checked_at": datetime.now(timezone.utc).isoformat(), "latest_main_close_time": latest_main})
        _save_state(state)
        return {"symbol": symbol, "status": "skipped", "reason": "already_has_required_4h_history", "latest_main_close_time": latest_main}

    pushed = 0
    upserted = 0
    chunks = 0
    errors: list[dict[str, Any]] = []
    while cursor < now_ms - FOUR_HOURS_MS and chunks < max_chunks:
        try:
            limiter.wait()
            candles = fetch_exchange_klines(getattr(settings, "exchange", "kraken"), settings.kraken_base_url if str(getattr(settings, "exchange", "kraken")).lower() in {"kraken", "kraken_pro"} else settings.kraken_base_url, symbol, INTERVAL, chunk_limit, start_time=cursor)
            candles = [candle for candle in candles if int(candle.get("open_time", 0)) >= cursor]
            if not candles:
                symbol_state.update({"status": "complete_or_no_more_exchange_data", "next_start_time": cursor, "last_checked_at": datetime.now(timezone.utc).isoformat()})
                _save_state(state)
                break
            response = client.post_candles(symbol, INTERVAL, candles, source=f"{settings.gateway_id}-backfill-4h-365d")
            pushed += len(candles)
            upserted += int(response.get("upserted") or 0)
            chunks += 1
            last_close = max(int(candle["close_time"]) for candle in candles)
            cursor = last_close + 1
            symbol_state.update({"status": "in_progress", "next_start_time": cursor, "last_close_time_sent": last_close, "last_response": response, "last_sent_at": datetime.now(timezone.utc).isoformat(), "days_target": days})
            _save_state(state)
            if post_sleep > 0:
                time.sleep(post_sleep)
            if len(candles) < chunk_limit:
                break
        except Exception as exc:
            errors.append({"symbol": symbol, "cursor": cursor, "error": str(exc)})
            symbol_state.update({"status": "error", "next_start_time": cursor, "last_error": str(exc), "last_error_at": datetime.now(timezone.utc).isoformat()})
            _save_state(state)
            break
    return {"symbol": symbol, "interval": INTERVAL, "status": "done" if not errors else "error", "fetched_missing": pushed, "posted": pushed, "upserted": upserted, "pushed": pushed, "chunks": chunks, "next_start_time": cursor, "errors": errors}


def run_once(days: int | None = None, max_symbols: int | None = None, max_chunks_per_symbol: int | None = None, enabled_override: bool = False) -> dict[str, Any]:
    ensure_env()
    env = read_env()
    settings = load_settings()
    enabled = enabled_override or _bool(env.get("BACKFILL_4H_ENABLED") or os.getenv("BACKFILL_4H_ENABLED"), default=False)
    if not enabled:
        return {"ok": False, "status": "disabled", "reason": "BACKFILL_4H_ENABLED=false"}
    days = int(days or env.get("BACKFILL_4H_DAYS", DEFAULT_DAYS) or DEFAULT_DAYS)
    chunk_limit = max(1, min(1000, int(env.get("BACKFILL_4H_CHUNK_LIMIT", 1000) or 1000)))
    max_chunks = max(1, int(max_chunks_per_symbol or env.get("BACKFILL_4H_MAX_CHUNKS_PER_SYMBOL", 3) or 3))
    post_sleep = max(0.0, float(env.get("BACKFILL_4H_POST_SLEEP", "0.2") or "0.2"))
    run_symbol_limit = int(max_symbols or env.get("BACKFILL_4H_MAX_SYMBOLS_PER_RUN", "10") or "10")
    client = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    symbols, quote_assets, mode = resolve_feed_symbols(settings)
    if run_symbol_limit > 0:
        symbols = symbols[:run_symbol_limit]
    if not symbols:
        return {"ok": False, "status": "blocked", "reason": "no_symbols_to_backfill", "quote_assets": quote_assets, "execution_mode": mode}
    endpoint_check = client.check_candle_ingest_endpoint(symbols[0], INTERVAL)
    if not endpoint_check.get("ok"):
        return {"ok": False, "status": "blocked", "endpoint_check": endpoint_check}
    exchange = getattr(settings, "exchange", "kraken")
    exchange_base_url = settings.kraken_base_url if str(exchange).lower() in {"kraken", "kraken_pro"} else settings.kraken_base_url
    if str(exchange).lower() in {"kraken", "kraken_pro"}:
        requests_per_minute = int(env.get("CANDLE_FEED_KRAKEN_REQUESTS_PER_MINUTE", "60") or "60")
        doc_limit = requests_per_minute
        weight_ratio = 1.0
    else:
        requests_per_minute, doc_limit, weight_ratio = effective_kraken_requests_per_minute(exchange_base_url, env)
    limiter = RateLimiter(requests_per_minute)
    state = _load_state()
    results = [backfill_symbol(settings, client, limiter, state, symbol, days=days, chunk_limit=chunk_limit, max_chunks=max_chunks, post_sleep=post_sleep) for symbol in symbols]
    total_posted = sum(int(item.get("posted") or item.get("pushed") or 0) for item in results)
    total_upserted = sum(int(item.get("upserted") or 0) for item in results)
    errors = [item for item in results if item.get("errors")]
    summary = {"ok": not errors, "status": "completed" if not errors else "partial", "symbol": symbols[0] if len(symbols) == 1 else None, "interval": INTERVAL, "fetched_missing": total_posted, "posted": total_posted, "upserted": total_upserted, "days": days, "symbols_requested": len(symbols), "quote_assets": quote_assets, "execution_mode": mode, "exchange_requests_per_minute": requests_per_minute, "kraken_doc_weight_limit_1m": doc_limit, "exchange": exchange, "weight_ratio": weight_ratio, "pushed": total_posted, "chunks": sum(int(item.get("chunks") or 0) for item in results), "errors": errors, "results": results, "completed_at": datetime.now(timezone.utc).isoformat()}
    state.setdefault("runs", []).append({k: v for k, v in summary.items() if k != "results"})
    state["runs"] = state["runs"][-20:]
    _save_state(state)
    return summary


def run_loop() -> None:
    ensure_env()
    env = read_env()
    if not _bool(env.get("BACKFILL_4H_ON_BOOT") or os.getenv("BACKFILL_4H_ON_BOOT"), default=False):
        logger.info("4h backfill loop disabled BACKFILL_4H_ON_BOOT=false")
        return
    poll_hours = max(1.0, float(env.get("BACKFILL_4H_POLL_HOURS", "24") or "24"))
    while True:
        try:
            result = run_once(enabled_override=True)
            logger.info("4h backfill result status=%s pushed=%s chunks=%s errors=%s", result.get("status"), result.get("pushed"), result.get("chunks"), len(result.get("errors") or []))
        except Exception as exc:
            logger.error("4h backfill error=%s", str(exc))
        time.sleep(poll_hours * 3600)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill 4h candles from Raspberry exchange to remote SignalMaker")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--max-chunks-per-symbol", type=int, default=None)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("symbol", nargs="?", help="Optional symbol for one-shot device backfill, e.g. BTCUSDC")
    parser.add_argument("interval", nargs="?", default=INTERVAL, help="Backfill interval; this device backfill currently posts 4h candles")
    args = parser.parse_args()
    if args.interval != INTERVAL:
        raise SystemExit("candle_backfill_4h supports interval 4h only")
    if args.symbol:
        import os
        os.environ["CANDLE_FEED_SYMBOLS"] = args.symbol.upper()
    print(json.dumps(run_once(days=args.days, max_symbols=1 if args.symbol else args.max_symbols, max_chunks_per_symbol=args.max_chunks_per_symbol, enabled_override=args.run), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    main()

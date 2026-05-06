import json
import os
import time
from pathlib import Path

import requests

from raspberry_executor.candle_push_once import fetch_klines
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ROOT, ensure_env, read_env
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.signalmaker_client import SignalMakerClient

logger = setup_logging("raspberry-candle-feed")
RETRY_PATH = ROOT / "raspberry_executor" / "candle_retry_queue.json"


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _csv(value: str | None, *, upper: bool = True) -> list[str]:
    items = [item.strip() for item in (value or "").split(",") if item.strip()]
    return [item.upper() for item in items] if upper else items


def _pair_key(symbol: str, interval: str) -> str:
    return f"{symbol.upper()}::{interval}"


def _load_retry_queue() -> dict[str, dict]:
    if not RETRY_PATH.exists():
        return {}
    try:
        data = json.loads(RETRY_PATH.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_retry_queue(queue: dict[str, dict]) -> None:
    RETRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    RETRY_PATH.write_text(json.dumps(queue, indent=2, sort_keys=True))


def _retry_items_first(symbols: list[str], intervals: list[str], queue: dict[str, dict]) -> list[tuple[str, str]]:
    allowed_symbols = {symbol.upper() for symbol in symbols}
    allowed_intervals = set(intervals)
    retry_pairs: list[tuple[str, str]] = []
    for item in sorted(queue.values(), key=lambda row: (int(row.get("attempts", 0)) * -1, row.get("last_error_at", ""))):
        symbol = str(item.get("symbol", "")).upper()
        interval = str(item.get("interval", ""))
        if symbol in allowed_symbols and interval in allowed_intervals:
            retry_pairs.append((symbol, interval))

    seen = set(retry_pairs)
    normal_pairs = [(symbol, interval) for symbol in symbols for interval in intervals if (symbol, interval) not in seen]
    return retry_pairs + normal_pairs


def _mark_retry(queue: dict[str, dict], symbol: str, interval: str, error: str) -> None:
    key = _pair_key(symbol, interval)
    current = queue.get(key, {})
    queue[key] = {
        "symbol": symbol.upper(),
        "interval": interval,
        "attempts": int(current.get("attempts", 0)) + 1,
        "last_error": str(error)[-500:],
        "last_error_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def _clear_retry(queue: dict[str, dict], symbol: str, interval: str) -> None:
    queue.pop(_pair_key(symbol, interval), None)


def discover_spot_symbols(base_url: str, quote_assets: list[str], limit: int = 0) -> list[str]:
    quotes = {quote.upper() for quote in quote_assets if quote.strip()}
    if not quotes:
        return []
    response = requests.get(f"{base_url.rstrip('/')}/api/v3/exchangeInfo", timeout=20)
    response.raise_for_status()
    data = response.json()
    symbols = []
    for row in data.get("symbols", []):
        symbol = str(row.get("symbol", "")).upper()
        quote_asset = str(row.get("quoteAsset", "")).upper()
        if row.get("status") != "TRADING":
            continue
        if quote_asset not in quotes:
            continue
        if not row.get("isSpotTradingAllowed", False):
            continue
        symbols.append(symbol)
    symbols = sorted(set(symbols))
    return symbols[:limit] if limit and limit > 0 else symbols


def resolve_feed_symbols(settings) -> tuple[list[str], list[str]]:
    env = read_env()
    quote_assets = settings.quote_assets or _csv(env.get("QUOTE_ASSETS", "USDT"))
    max_symbols = int(env.get("CANDLE_FEED_MAX_SYMBOLS", "0") or "0")
    return discover_spot_symbols(settings.binance_base_url, quote_assets, limit=max_symbols), quote_assets


def _start_time_from_latest(latest: dict | None) -> int | None:
    if not latest:
        return None
    close_time = latest.get("close_time")
    if close_time is None:
        return None
    return int(close_time) + 1


def run_once() -> dict:
    ensure_env()
    settings = load_settings()
    client = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    endpoint_check = client.check_candle_ingest_endpoint()
    if not endpoint_check.get("ok"):
        return {"status": "blocked", "endpoint_check": endpoint_check, "pushed": [], "skipped": [], "errors": []}

    symbols, quote_assets = resolve_feed_symbols(settings)
    intervals = _csv(os.getenv("CANDLE_FEED_INTERVALS", "15m,1h,4h"), upper=False)
    limit = int(os.getenv("CANDLE_FEED_LIMIT", "120"))
    retry_queue = _load_retry_queue()

    if not symbols:
        return {"status": "skipped", "reason": "no_symbols_configured", "quote_assets": quote_assets, "intervals": intervals, "retry_queue_size": len(retry_queue)}

    pushed = []
    skipped = []
    errors = []
    processed_pairs = _retry_items_first(symbols, intervals, retry_queue)

    for symbol, interval in processed_pairs:
        try:
            latest = client.latest_candle(symbol, interval)
            start_time = _start_time_from_latest(latest)
            candles = fetch_klines(settings.binance_base_url, symbol, interval, limit, start_time=start_time)
            if not candles:
                skipped.append({"symbol": symbol, "interval": interval, "reason": "no_missing_candles", "latest_close_time": latest.get("close_time") if latest else None})
                _clear_retry(retry_queue, symbol, interval)
                continue
            if start_time is not None:
                candles = [candle for candle in candles if int(candle["open_time"]) > int(latest["open_time"])]
            if not candles:
                skipped.append({"symbol": symbol, "interval": interval, "reason": "already_up_to_date", "latest_close_time": latest.get("close_time") if latest else None})
                _clear_retry(retry_queue, symbol, interval)
                continue
            response = client.post_candles(symbol, interval, candles, source=settings.gateway_id)
            pushed.append({
                "symbol": symbol,
                "interval": interval,
                "count": len(candles),
                "start_time": start_time,
                "upserted": response.get("upserted"),
                "was_retry": _pair_key(symbol, interval) in retry_queue,
            })
            _clear_retry(retry_queue, symbol, interval)
        except Exception as exc:
            _mark_retry(retry_queue, symbol, interval, str(exc))
            errors.append({"symbol": symbol, "interval": interval, "error": str(exc), "retry_queued": True})

    _save_retry_queue(retry_queue)
    return {
        "status": "ok" if not errors else "partial",
        "symbol_count": len(symbols),
        "quote_assets": quote_assets,
        "intervals": intervals,
        "pushed": pushed,
        "skipped": skipped,
        "errors": errors,
        "retry_queue_size": len(retry_queue),
        "retry_queue_path": str(RETRY_PATH),
    }


def run_loop() -> None:
    ensure_env()
    enabled = _bool(os.getenv("CANDLE_FEED_ENABLED"), default=True)
    if not enabled:
        logger.info("candle feed disabled by CANDLE_FEED_ENABLED=false")
        return

    poll_seconds = int(os.getenv("CANDLE_FEED_POLL_SECONDS", "60"))
    logger.info(
        "candle feed started quote_assets=%s intervals=%s poll_seconds=%s",
        os.getenv("QUOTE_ASSETS", "USDT"),
        os.getenv("CANDLE_FEED_INTERVALS", "15m,1h,4h"),
        poll_seconds,
    )

    while True:
        try:
            result = run_once()
            logger.info("candle feed result=%s", result)
        except Exception:
            logger.exception("candle feed loop error")
        time.sleep(poll_seconds)


if __name__ == "__main__":
    print(run_once())

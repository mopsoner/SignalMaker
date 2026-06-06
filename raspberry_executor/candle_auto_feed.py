import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from raspberry_executor.candle_push_once import fetch_klines
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ROOT, ensure_env, read_env
from raspberry_executor.feed_run_store import record_feed_run
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.margin_settings import execution_mode
from raspberry_executor.signalmaker_client import SignalMakerClient

logger = setup_logging("raspberry-candle-feed")
RETRY_PATH = ROOT / "raspberry_executor" / "candle_retry_queue.json"


class RateLimiter:
    def __init__(self, calls_per_minute: int) -> None:
        self.calls_per_minute = max(1, int(calls_per_minute))
        self.min_interval = 60.0 / float(self.calls_per_minute)
        self.lock = threading.Lock()
        self.next_allowed_at = 0.0

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            if now < self.next_allowed_at:
                time.sleep(self.next_allowed_at - now)
                now = time.monotonic()
            self.next_allowed_at = now + self.min_interval


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
    queue[key] = {"symbol": symbol.upper(), "interval": interval, "attempts": int(current.get("attempts", 0)) + 1, "last_error": str(error)[-500:], "last_error_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


def _clear_retry(queue: dict[str, dict], symbol: str, interval: str) -> None:
    queue.pop(_pair_key(symbol, interval), None)


def _exchange_info(base_url: str) -> dict:
    response = requests.get(f"{base_url.rstrip('/')}/api/v3/exchangeInfo", timeout=20)
    response.raise_for_status()
    return response.json()


def _public_get(base_url: str, path: str, params: dict | None = None, api_key: str | None = None) -> dict | list:
    headers = {"X-MBX-APIKEY": api_key} if api_key else {}
    response = requests.get(f"{base_url.rstrip('/')}{path}", params=params or {}, headers=headers, timeout=20)
    if not response.ok:
        raise RuntimeError(f"Binance GET {path} failed status={response.status_code} body={response.text[:300]}")
    return response.json()


def binance_request_weight_limit_per_minute(base_url: str) -> int:
    data = _exchange_info(base_url)
    for item in data.get("rateLimits", []):
        if item.get("rateLimitType") == "REQUEST_WEIGHT" and item.get("interval") == "MINUTE" and int(item.get("intervalNum", 1)) == 1:
            return int(item.get("limit"))
    return 6000


def effective_binance_requests_per_minute(base_url: str, env: dict[str, str]) -> tuple[int, int, float]:
    doc_limit = binance_request_weight_limit_per_minute(base_url)
    ratio = float(env.get("CANDLE_FEED_BINANCE_REQUEST_WEIGHT_RATIO", "0.60") or "0.60")
    ratio = min(max(ratio, 0.05), 0.95)
    override = int(env.get("CANDLE_FEED_BINANCE_REQUESTS_PER_MINUTE", "0") or "0")
    computed = max(1, int(doc_limit * ratio))
    if override > 0:
        return min(override, computed), doc_limit, ratio
    return computed, doc_limit, ratio


def discover_spot_symbols(base_url: str, quote_assets: list[str], limit: int = 0) -> list[str]:
    quotes = {quote.upper() for quote in quote_assets if quote.strip()}
    if not quotes:
        return []
    data = _exchange_info(base_url)
    symbols = []
    for row in data.get("symbols", []):
        symbol = str(row.get("symbol", "")).upper()
        quote_asset = str(row.get("quoteAsset", "")).upper()
        if row.get("status") != "TRADING" or quote_asset not in quotes:
            continue
        if not row.get("isSpotTradingAllowed", False):
            continue
        symbols.append(symbol)
    symbols = sorted(set(symbols))
    return symbols[:limit] if limit and limit > 0 else symbols


def discover_cross_margin_symbols(base_url: str, quote_assets: list[str], limit: int = 0, api_key: str | None = None) -> list[str]:
    quotes = {quote.upper() for quote in quote_assets if quote.strip()}
    if not quotes:
        return []
    data = _public_get(base_url, "/sapi/v1/margin/allPairs", api_key=api_key)
    symbols = []
    for row in data if isinstance(data, list) else []:
        symbol = str(row.get("symbol", "")).upper()
        quote = str(row.get("quote", "")).upper()
        if quote not in quotes:
            continue
        if row.get("isMarginTrade") is False:
            continue
        if row.get("isBuyAllowed") is False or row.get("isSellAllowed") is False:
            continue
        symbols.append(symbol)
    symbols = sorted(set(symbols))
    return symbols[:limit] if limit and limit > 0 else symbols


def discover_isolated_margin_symbols(base_url: str, quote_assets: list[str], limit: int = 0, api_key: str | None = None) -> list[str]:
    quotes = {quote.upper() for quote in quote_assets if quote.strip()}
    if not quotes:
        return []
    data = _public_get(base_url, "/sapi/v1/margin/isolated/allPairs", api_key=api_key)
    symbols = []
    for row in data if isinstance(data, list) else []:
        symbol = str(row.get("symbol", "")).upper()
        quote = str(row.get("quote", "")).upper()
        if quote not in quotes:
            continue
        if row.get("isBuyAllowed") is False or row.get("isSellAllowed") is False:
            continue
        symbols.append(symbol)
    symbols = sorted(set(symbols))
    return symbols[:limit] if limit and limit > 0 else symbols


def discover_symbols_for_execution_mode(base_url: str, quote_assets: list[str], mode: str, limit: int = 0, api_key: str | None = None) -> tuple[list[str], str]:
    if mode == "cross":
        try:
            return discover_cross_margin_symbols(base_url, quote_assets, limit=limit, api_key=api_key), "cross_margin"
        except Exception as exc:
            logger.warning("cross margin symbol discovery failed; falling back to spot candle symbols error=%s", str(exc))
            return discover_spot_symbols(base_url, quote_assets, limit=limit), "spot_fallback_for_cross"
    if mode == "isolated":
        try:
            return discover_isolated_margin_symbols(base_url, quote_assets, limit=limit, api_key=api_key), "isolated_margin"
        except Exception as exc:
            logger.warning("isolated margin symbol discovery failed; falling back to spot candle symbols error=%s", str(exc))
            return discover_spot_symbols(base_url, quote_assets, limit=limit), "spot_fallback_for_isolated"
    return discover_spot_symbols(base_url, quote_assets, limit=limit), "spot"


def resolve_feed_symbols(settings) -> tuple[list[str], list[str], str]:
    env = read_env()
    quote_assets = settings.quote_assets or _csv(env.get("QUOTE_ASSETS", "USDT"))
    max_symbols = int(env.get("CANDLE_FEED_MAX_SYMBOLS", "0") or "0")
    mode = execution_mode()
    symbols, source = discover_symbols_for_execution_mode(settings.binance_base_url, quote_assets, mode, limit=max_symbols, api_key=settings.binance_api_key)
    return symbols, quote_assets, f"{mode}:{source}"


def _start_time_from_latest(latest: dict | None) -> int | None:
    if not latest:
        return None
    close_time = latest.get("close_time")
    return None if close_time is None else int(close_time) + 1


def _process_pair(settings, client: SignalMakerClient, limiter: RateLimiter, symbol: str, interval: str, limit: int) -> dict:
    latest = client.latest_candle(symbol, interval)
    start_time = _start_time_from_latest(latest)
    limiter.wait()
    candles = fetch_klines(settings.binance_base_url, symbol, interval, limit, start_time=start_time)
    if not candles:
        return {"kind": "skipped", "symbol": symbol, "interval": interval, "reason": "no_missing_candles", "latest_close_time": latest.get("close_time") if latest else None}
    if start_time is not None:
        candles = [candle for candle in candles if int(candle["open_time"]) > int(latest["open_time"])]
    if not candles:
        return {"kind": "skipped", "symbol": symbol, "interval": interval, "reason": "already_up_to_date", "latest_close_time": latest.get("close_time") if latest else None}
    response = client.post_candles(symbol, interval, candles, source=settings.gateway_id)
    return {"kind": "pushed", "symbol": symbol, "interval": interval, "count": len(candles), "start_time": start_time, "upserted": response.get("upserted")}


def _record_feed_status(status: str, **extra) -> None:
    payload = {"status": status, "pushed": [], "skipped": [], "errors": [], **extra}
    try:
        record_feed_run(payload)
    except Exception as exc:
        logger.warning("unable to record candle feed status=%s error=%s", status, str(exc))


def run_once() -> dict:
    ensure_env()
    env = read_env()
    settings = load_settings()
    intervals = _csv(env.get("CANDLE_FEED_INTERVALS", "15m,1h,4h"), upper=False)
    _record_feed_status(
        "running",
        reason="run_started",
        execution_mode=execution_mode(),
        quote_assets=_csv(env.get("QUOTE_ASSETS", "USDT")),
        intervals=intervals,
    )
    client = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    endpoint_check = client.check_candle_ingest_endpoint()
    if not endpoint_check.get("ok"):
        result = {"status": "blocked", "endpoint_check": endpoint_check, "pushed": [], "skipped": [], "errors": []}
        record_feed_run(result)
        return result

    symbols, quote_assets, mode = resolve_feed_symbols(settings)
    limit = int(env.get("CANDLE_FEED_LIMIT", "120") or "120")
    max_workers = max(1, int(env.get("CANDLE_FEED_MAX_WORKERS", "3") or "3"))
    requests_per_minute, binance_doc_weight_limit_1m, weight_ratio = effective_binance_requests_per_minute(settings.binance_base_url, env)
    retry_queue = _load_retry_queue()

    if not symbols:
        result = {"status": "skipped", "reason": "no_symbols_for_execution_mode", "execution_mode": mode, "quote_assets": quote_assets, "intervals": intervals, "retry_queue_size": len(retry_queue), "pushed": [], "skipped": [], "errors": []}
        record_feed_run(result)
        return result

    pushed = []
    skipped = []
    errors = []
    limiter = RateLimiter(requests_per_minute)
    processed_pairs = _retry_items_first(symbols, intervals, retry_queue)
    worker_count = min(max_workers, max(1, len(processed_pairs)))

    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        futures = {pool.submit(_process_pair, settings, client, limiter, symbol, interval, limit): (symbol, interval) for symbol, interval in processed_pairs}
        for future in as_completed(futures):
            symbol, interval = futures[future]
            try:
                result = future.result()
                if result.get("kind") == "pushed":
                    result.pop("kind", None)
                    result["was_retry"] = _pair_key(symbol, interval) in retry_queue
                    pushed.append(result)
                    _clear_retry(retry_queue, symbol, interval)
                else:
                    result.pop("kind", None)
                    skipped.append(result)
                    _clear_retry(retry_queue, symbol, interval)
            except Exception as exc:
                error_text = str(exc)
                if "429" in error_text or "418" in error_text:
                    logger.warning("Binance rate-limit response seen; reduce pressure error=%s", error_text)
                _mark_retry(retry_queue, symbol, interval, error_text)
                errors.append({"symbol": symbol, "interval": interval, "error": error_text, "retry_queued": True})

    _save_retry_queue(retry_queue)
    result = {"status": "ok" if not errors else "partial", "execution_mode": mode, "symbol_count": len(symbols), "quote_assets": quote_assets, "intervals": intervals, "max_workers": worker_count, "binance_doc_request_weight_limit_1m": binance_doc_weight_limit_1m, "binance_request_weight_ratio": weight_ratio, "binance_effective_requests_per_minute": requests_per_minute, "pushed": pushed, "skipped": skipped, "errors": errors, "retry_queue_size": len(retry_queue), "retry_queue_path": str(RETRY_PATH)}
    record_feed_run(result)
    return result


def run_loop() -> None:
    ensure_env()
    env = read_env()
    enabled = _bool(env.get("CANDLE_FEED_ENABLED"), default=True)
    if not enabled:
        logger.info("candle feed disabled by CANDLE_FEED_ENABLED=false")
        _record_feed_status("disabled", reason="CANDLE_FEED_ENABLED=false")
        return

    poll_seconds = int(env.get("CANDLE_FEED_POLL_SECONDS", "60") or "60")
    try:
        doc_limit = binance_request_weight_limit_per_minute(load_settings().binance_base_url)
    except Exception:
        doc_limit = 6000
    logger.info("candle feed started execution_mode=%s quote_assets=%s intervals=%s poll_seconds=%s max_workers=%s binance_doc_weight_limit_1m=%s weight_ratio=%s rpm_override=%s", execution_mode(), env.get("QUOTE_ASSETS", "USDT"), env.get("CANDLE_FEED_INTERVALS", "15m,1h,4h"), poll_seconds, env.get("CANDLE_FEED_MAX_WORKERS", "3"), doc_limit, env.get("CANDLE_FEED_BINANCE_REQUEST_WEIGHT_RATIO", "0.60"), env.get("CANDLE_FEED_BINANCE_REQUESTS_PER_MINUTE", "0"))

    while True:
        try:
            result = run_once()
            logger.info("candle feed result=%s", result)
        except Exception as exc:
            logger.error("candle feed loop error=%s", str(exc))
            try:
                record_feed_run({"status": "error", "pushed": [], "skipped": [], "errors": [{"error": str(exc)}]})
            except Exception:
                pass
        time.sleep(poll_seconds)


if __name__ == "__main__":
    print(run_once())

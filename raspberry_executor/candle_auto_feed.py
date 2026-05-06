import os
import time

import requests

from raspberry_executor.candle_push_once import fetch_klines
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ensure_env, read_env
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.signalmaker_client import SignalMakerClient

logger = setup_logging("raspberry-candle-feed")


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _csv(value: str | None, *, upper: bool = True) -> list[str]:
    items = [item.strip() for item in (value or "").split(",") if item.strip()]
    return [item.upper() for item in items] if upper else items


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

    if not symbols:
        return {"status": "skipped", "reason": "no_symbols_configured", "quote_assets": quote_assets, "intervals": intervals}

    pushed = []
    skipped = []
    errors = []

    for symbol in symbols:
        for interval in intervals:
            try:
                latest = client.latest_candle(symbol, interval)
                start_time = _start_time_from_latest(latest)
                candles = fetch_klines(settings.binance_base_url, symbol, interval, limit, start_time=start_time)
                if not candles:
                    skipped.append({"symbol": symbol, "interval": interval, "reason": "no_missing_candles", "latest_close_time": latest.get("close_time") if latest else None})
                    continue
                if start_time is not None:
                    candles = [candle for candle in candles if int(candle["open_time"]) > int(latest["open_time"])]
                if not candles:
                    skipped.append({"symbol": symbol, "interval": interval, "reason": "already_up_to_date", "latest_close_time": latest.get("close_time") if latest else None})
                    continue
                response = client.post_candles(symbol, interval, candles, source=settings.gateway_id)
                pushed.append({
                    "symbol": symbol,
                    "interval": interval,
                    "count": len(candles),
                    "start_time": start_time,
                    "upserted": response.get("upserted"),
                })
            except Exception as exc:
                errors.append({"symbol": symbol, "interval": interval, "error": str(exc)})

    return {
        "status": "ok" if not errors else "partial",
        "symbol_count": len(symbols),
        "quote_assets": quote_assets,
        "intervals": intervals,
        "pushed": pushed,
        "skipped": skipped,
        "errors": errors,
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

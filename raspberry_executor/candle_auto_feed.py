import os
import time

import requests

from raspberry_executor.candle_push_once import fetch_klines
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ensure_env
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.signalmaker_client import SignalMakerClient

logger = setup_logging("raspberry-candle-feed")
KNOWN_QUOTES = {"USDT", "USDC", "FDUSD", "BUSD", "TUSD", "USD", "EUR", "BTC", "ETH", "BNB"}


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
    # Existing env first: ALLOWED_SYMBOLS can contain either full symbols or quote assets.
    # Example: ALLOWED_SYMBOLS=ACHUSDT,BTCUSDT feeds only those symbols.
    # Example: ALLOWED_SYMBOLS=USDT discovers all tradable spot pairs quoted in USDT.
    configured = settings.allowed_symbols
    explicit_symbols = [item for item in configured if item not in KNOWN_QUOTES]
    quote_assets = [item for item in configured if item in KNOWN_QUOTES]

    # Optional override, but not required. Existing ALLOWED_SYMBOLS remains the main config.
    explicit_symbols.extend(_csv(os.getenv("CANDLE_FEED_SYMBOLS")))
    quote_assets.extend(_csv(os.getenv("CANDLE_FEED_QUOTE_ASSETS")))

    explicit_symbols = sorted(set(explicit_symbols))
    quote_assets = sorted(set(quote_assets))
    max_symbols = int(os.getenv("CANDLE_FEED_MAX_SYMBOLS", "0"))

    if quote_assets:
        discovered = discover_spot_symbols(settings.binance_base_url, quote_assets, limit=max_symbols)
        return sorted(set(explicit_symbols + discovered)), quote_assets

    return explicit_symbols, quote_assets


def run_once() -> dict:
    ensure_env()
    settings = load_settings()
    client = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    endpoint_check = client.check_candle_ingest_endpoint()
    if not endpoint_check.get("ok"):
        return {"status": "blocked", "endpoint_check": endpoint_check, "pushed": [], "errors": []}

    symbols, quote_assets = resolve_feed_symbols(settings)
    intervals = _csv(os.getenv("CANDLE_FEED_INTERVALS", "15m,1h,4h"), upper=False)
    limit = int(os.getenv("CANDLE_FEED_LIMIT", "120"))

    if not symbols:
        return {"status": "skipped", "reason": "no_symbols_configured", "intervals": intervals}

    pushed = []
    errors = []

    for symbol in symbols:
        for interval in intervals:
            try:
                candles = fetch_klines(settings.binance_base_url, symbol, interval, limit)
                if not candles:
                    errors.append({"symbol": symbol, "interval": interval, "error": "no_candles"})
                    continue
                response = client.post_candles(symbol, interval, candles, source=settings.gateway_id)
                pushed.append({
                    "symbol": symbol,
                    "interval": interval,
                    "count": len(candles),
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
        "candle feed started allowed_symbols=%s intervals=%s poll_seconds=%s",
        os.getenv("ALLOWED_SYMBOLS", ""),
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

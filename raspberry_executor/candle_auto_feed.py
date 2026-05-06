import os
import time

from raspberry_executor.candle_push_once import fetch_klines
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ensure_env
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


def run_once() -> dict:
    ensure_env()
    settings = load_settings()
    symbols = _csv(os.getenv("CANDLE_FEED_SYMBOLS")) or settings.allowed_symbols
    intervals = _csv(os.getenv("CANDLE_FEED_INTERVALS", "15m,1h,4h"), upper=False)
    limit = int(os.getenv("CANDLE_FEED_LIMIT", "120"))

    if not symbols:
        return {"status": "skipped", "reason": "no_symbols_configured", "intervals": intervals}

    client = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
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

    return {"status": "ok" if not errors else "partial", "pushed": pushed, "errors": errors}


def run_loop() -> None:
    ensure_env()
    enabled = _bool(os.getenv("CANDLE_FEED_ENABLED"), default=True)
    if not enabled:
        logger.info("candle feed disabled by CANDLE_FEED_ENABLED=false")
        return

    poll_seconds = int(os.getenv("CANDLE_FEED_POLL_SECONDS", "60"))
    logger.info("candle feed started intervals=%s poll_seconds=%s", os.getenv("CANDLE_FEED_INTERVALS", "15m,1h,4h"), poll_seconds)

    while True:
        try:
            result = run_once()
            logger.info("candle feed result=%s", result)
        except Exception:
            logger.exception("candle feed loop error")
        time.sleep(poll_seconds)


if __name__ == "__main__":
    print(run_once())

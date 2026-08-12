#!/usr/bin/env python3
"""Run the internal Kraken candle importer once or at a fixed interval."""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import threading
from dataclasses import dataclass
from typing import Mapping

from app.db.session import SessionLocal
from app.services.kraken_candle_importer import import_kraken_candles


logger = logging.getLogger("kraken-candle-feed")


def _as_bool(value: str | None, *, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _csv(value: str | None, *, default: list[str]) -> list[str]:
    items = [item.strip() for item in (value or "").split(",") if item.strip()]
    return items or default


def _positive_int(value: str | None, *, default: int, allow_zero: bool = False) -> int:
    try:
        parsed = int(value) if value is not None else default
    except ValueError as exc:
        raise ValueError(f"Expected an integer, got {value!r}") from exc
    minimum = 0 if allow_zero else 1
    if parsed < minimum:
        raise ValueError(f"Expected a value >= {minimum}, got {parsed}")
    return parsed


@dataclass(frozen=True)
class FeedSettings:
    enabled: bool
    poll_seconds: int
    intervals: list[str]
    quote_assets: list[str]
    limit: int
    max_symbols: int
    margin_only: bool

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "FeedSettings":
        values = os.environ if env is None else env
        return cls(
            enabled=_as_bool(values.get("KRAKEN_CANDLE_FEED_ENABLED"), default=True),
            poll_seconds=_positive_int(values.get("KRAKEN_CANDLE_FEED_POLL_SECONDS"), default=60),
            intervals=_csv(values.get("KRAKEN_CANDLE_FEED_INTERVALS"), default=["15m", "1h", "4h"]),
            quote_assets=[
                item.upper()
                for item in _csv(values.get("KRAKEN_CANDLE_FEED_QUOTE_ASSETS"), default=["USD"])
            ],
            limit=_positive_int(values.get("KRAKEN_CANDLE_FEED_LIMIT"), default=120),
            max_symbols=_positive_int(
                values.get("KRAKEN_CANDLE_FEED_MAX_SYMBOLS"), default=0, allow_zero=True
            ),
            margin_only=_as_bool(values.get("KRAKEN_CANDLE_FEED_MARGIN_ONLY"), default=True),
        )


def run_once(settings: FeedSettings | None = None) -> dict:
    settings = settings or FeedSettings.from_env()
    if not settings.enabled:
        return {"status": "disabled", "source": "kraken_internal", "reason": "KRAKEN_CANDLE_FEED_ENABLED=false"}

    db = SessionLocal()
    try:
        return import_kraken_candles(
            db=db,
            quote_assets=settings.quote_assets,
            intervals=settings.intervals,
            limit=settings.limit,
            max_symbols=settings.max_symbols,
            margin_only=settings.margin_only,
        )
    finally:
        db.close()


def run_loop(settings: FeedSettings | None = None, stop_event: threading.Event | None = None) -> None:
    settings = settings or FeedSettings.from_env()
    stop_event = stop_event or threading.Event()
    if not settings.enabled:
        logger.info("Kraken candle feed disabled by KRAKEN_CANDLE_FEED_ENABLED=false")
        return

    logger.info(
        "Kraken candle feed started intervals=%s quote_assets=%s poll_seconds=%s",
        settings.intervals,
        settings.quote_assets,
        settings.poll_seconds,
    )
    while not stop_event.is_set():
        try:
            logger.info("Kraken candle feed result=%s", json.dumps(run_once(settings), default=str))
        except Exception:
            logger.exception("Kraken candle feed iteration failed")
        stop_event.wait(settings.poll_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Run one import and exit instead of polling.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    settings = FeedSettings.from_env()
    if args.once:
        result = run_once(settings)
        print(json.dumps(result, indent=2, default=str))
        return 1 if result.get("status") in {"error", "partial"} else 0

    stopping = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda _signum, _frame: stopping.set())
    run_loop(settings, stopping)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

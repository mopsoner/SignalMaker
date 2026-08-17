#!/usr/bin/env python3
"""Submit ready Wyckoff/SMC candidates to Kraken from a dedicated live worker."""

import json
import time

from app.core.config import settings
from app.db.session import SessionLocal
from app.services.executor_service import ExecutorService


def assert_live_configuration() -> None:
    problems = []
    mode = settings.wyckoff_live_mode.lower()
    if not settings.wyckoff_live_enabled:
        problems.append("WYCKOFF_LIVE_ENABLED must be true")
    if not settings.kraken_execution_enabled:
        problems.append("KRAKEN_EXECUTION_ENABLED must be true")
    if settings.kraken_dry_run:
        problems.append("KRAKEN_DRY_RUN must be false")
    if not settings.kraken_api_key or not settings.kraken_secret_key:
        problems.append("KRAKEN_API_KEY and KRAKEN_SECRET_KEY are required")
    if mode not in {"spot", "margin"}:
        problems.append("WYCKOFF_LIVE_MODE must be spot or margin")
    if mode == "margin" and not settings.kraken_margin_execution_enabled:
        problems.append("KRAKEN_MARGIN_EXECUTION_ENABLED must be true for margin mode")
    if problems:
        raise RuntimeError("Unsafe/incomplete live Wyckoff/SMC configuration: " + "; ".join(problems))


def run_once() -> dict:
    with SessionLocal() as db:
        result = ExecutorService(db).execute_open_candidates(
            limit=settings.wyckoff_live_limit,
            quantity=settings.wyckoff_live_quantity,
            mode="live",
        )
        db.commit()
        return result


if __name__ == "__main__":
    assert_live_configuration()
    once = "--once" in __import__("sys").argv
    while True:
        print(json.dumps(run_once(), default=str), flush=True)
        if once:
            break
        time.sleep(settings.wyckoff_live_interval_seconds)

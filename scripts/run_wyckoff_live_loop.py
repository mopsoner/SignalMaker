#!/usr/bin/env python3
"""Submit ready Wyckoff/SMC candidates to Kraken from a dedicated live worker."""

import json
import time

from app.core.config import settings
from app.db.session import SessionLocal
from app.services.executor_service import ExecutorService
from app.services.execution.live_configuration import assert_wyckoff_live_configuration


def assert_live_configuration() -> None:
    assert_wyckoff_live_configuration(settings)


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

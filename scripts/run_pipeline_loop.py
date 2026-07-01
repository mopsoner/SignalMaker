#!/usr/bin/env python3
"""
Pipeline worker — runs PipelineService.run_once() on a configurable interval.
Reads interval and enabled flag from runtime settings (DB) at each tick so
changes made in the Admin Settings page take effect without a restart.
"""
import os
import sys
import time
from typing import Any

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.session import SessionLocal
from app.services.pipeline_service import PipelineService
from app.services.runtime_settings import load_runtime_settings

DEFAULT_INTERVAL = 60
DEFAULT_LIMIT = 25


def _resolve_pipeline_limit(runtime: dict[str, dict[str, Any]]) -> int:
    """Resolve the pipeline symbol limit from canonical or legacy runtime settings."""
    market_data = runtime.get("market_data", {})
    kraken = runtime.get("kraken", {})
    return int(market_data.get("kraken_max_symbols", kraken.get("kraken_max_symbols", DEFAULT_LIMIT)))


def _run_pipeline_tick(db: Any) -> int:
    """Run one pipeline worker tick and return the next sleep interval in seconds."""
    runtime = load_runtime_settings(db)
    bot = runtime.get("bot", {})

    if not bot.get("bot_pipeline_enabled", True):
        print("Pipeline disabled — sleeping 30s", flush=True)
        return 30

    limit = _resolve_pipeline_limit(runtime)
    interval = int(bot.get("bot_pipeline_interval_sec", DEFAULT_INTERVAL))

    result = PipelineService(db).run_once(limit=limit)
    print(f"Pipeline tick: {result}", flush=True)
    return interval


if __name__ == "__main__":
    print("Pipeline worker started", flush=True)
    while True:
        db = SessionLocal()
        try:
            interval = _run_pipeline_tick(db)
        except Exception as exc:
            print(f"Pipeline error: {exc}", flush=True)
            interval = 30
        finally:
            try:
                db.close()
            except Exception:
                pass

        time.sleep(interval)

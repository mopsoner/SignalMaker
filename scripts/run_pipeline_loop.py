#!/usr/bin/env python3
"""
Pipeline worker — runs PipelineService.run_once() on a configurable interval.
Reads interval and enabled flag from runtime settings (DB) at each tick so
changes made in the Admin Settings page take effect without a restart.
"""
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.session import SessionLocal
from app.services.pipeline_service import PipelineService
from app.services.runtime_settings import load_runtime_settings

DEFAULT_INTERVAL = 60
DEFAULT_LIMIT = 25

if __name__ == "__main__":
    print("Pipeline worker started", flush=True)
    while True:
        db = SessionLocal()
        try:
            runtime = load_runtime_settings(db)
            bot = runtime.get("bot", {})
            binance = runtime.get("binance", {})

            if not bot.get("bot_pipeline_enabled", True):
                print("Pipeline disabled — sleeping 30s", flush=True)
                time.sleep(30)
                continue

            limit = int(binance.get("binance_max_symbols", DEFAULT_LIMIT))
            interval = int(bot.get("bot_pipeline_interval_sec", DEFAULT_INTERVAL))

            result = PipelineService(db).run_once(limit=limit)
            print(f"Pipeline tick: {result}", flush=True)

        except Exception as exc:
            print(f"Pipeline error: {exc}", flush=True)
            interval = 30
        finally:
            try:
                db.close()
            except Exception:
                pass

        time.sleep(interval)

#!/usr/bin/env python3
"""
Pipeline worker — runs PipelineService.run_once() on a configurable interval.
Reads interval and enabled flag from runtime settings (DB) at each tick so
changes made in the Admin Settings page take effect without a restart.
"""
import os
import sys
import time
import traceback
import logging

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.session import SessionLocal
from app.services.pipeline_service import PipelineService
from app.services.runtime_settings import load_runtime_settings

DEFAULT_INTERVAL = 60
DEFAULT_LIMIT = 25

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def as_bool(value, default=True):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return default

if __name__ == "__main__":
    print("Pipeline worker started", flush=True)
    while True:
        db = SessionLocal()
        try:
            runtime = load_runtime_settings(db)
            bot = runtime.get("bot", {})

            enabled = as_bool(bot.get("bot_pipeline_enabled", True), default=True)
            limit = DEFAULT_LIMIT
            interval = int(bot.get("bot_pipeline_interval_sec", DEFAULT_INTERVAL))
            settings_log = (
                f"bot_pipeline_enabled={enabled} "
                f"bot_pipeline_interval_sec={interval} "
                f"symbol_limit={limit}"
            )

            print(f"Pipeline tick start: {settings_log}", flush=True)
            if not enabled:
                print(f"Pipeline disabled: {settings_log} — sleeping 30s", flush=True)
                time.sleep(30)
                continue

            result = PipelineService(db).run_once(limit=limit)
            print(f"Pipeline tick complete: {result}", flush=True)

        except Exception as exc:
            print(f"Pipeline error: {exc}", flush=True)
            traceback.print_exc()
            interval = 30
        finally:
            try:
                db.close()
            except Exception:
                pass

        time.sleep(interval)

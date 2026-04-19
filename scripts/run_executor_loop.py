#!/usr/bin/env python3
"""
Executor worker — runs ExecutorService.execute_open_candidates() on a
configurable interval. Reads config from runtime settings at each tick.
"""
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.core.config import settings
from app.db.session import SessionLocal
from app.services.executor_service import ExecutorService
from app.services.runtime_settings import load_runtime_settings

DEFAULT_INTERVAL = 30
DEFAULT_LIMIT = 10
DEFAULT_QUANTITY = 1.0

if __name__ == "__main__":
    print("Executor worker started", flush=True)
    while True:
        db = SessionLocal()
        try:
            runtime = load_runtime_settings(db)
            bot = runtime.get("bot", {})
            live_cfg = runtime.get("live", {})

            if not bot.get("bot_executor_enabled", True):
                print("Executor disabled — sleeping 30s", flush=True)
                time.sleep(30)
                continue

            limit = int(bot.get("bot_executor_limit", DEFAULT_LIMIT))
            quantity = float(bot.get("bot_executor_quantity", DEFAULT_QUANTITY))
            interval = int(bot.get("bot_executor_interval_sec", DEFAULT_INTERVAL))
            mode = 'live' if live_cfg.get('live_trading_enabled', settings.live_trading_enabled) else 'paper'

            result = ExecutorService(db).execute_open_candidates(limit=limit, quantity=quantity, mode=mode)
            print(f"Executor tick ({mode}): {result}", flush=True)

        except Exception as exc:
            print(f"Executor error: {exc}", flush=True)
            interval = 30
        finally:
            try:
                db.close()
            except Exception:
                pass

        time.sleep(interval)

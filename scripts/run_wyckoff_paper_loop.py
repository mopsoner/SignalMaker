#!/usr/bin/env python3
"""
Wyckoff/SMC paper worker — runs ExecutorService.execute_open_candidates() on a
configurable interval. Reads config from runtime settings at each tick.
"""
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.session import SessionLocal
from app.services.executor_service import ExecutorService
from app.services.runtime_settings import load_runtime_settings

DEFAULT_INTERVAL = 30
DEFAULT_LIMIT = 10
DEFAULT_QUANTITY = 1.0

if __name__ == "__main__":
    print("Wyckoff/SMC paper worker started", flush=True)
    while True:
        db = SessionLocal()
        try:
            runtime = load_runtime_settings(db)
            bot = runtime.get("bot", {})
            live_cfg = runtime.get("live", {})

            if not bot.get("bot_wyckoff_paper_enabled", True):
                print("Wyckoff/SMC paper worker disabled — sleeping 30s", flush=True)
                db.rollback()
                db.close()
                time.sleep(30)
                continue

            limit = int(bot.get("bot_wyckoff_paper_limit", DEFAULT_LIMIT))
            quantity = float(bot.get("bot_wyckoff_paper_quantity", DEFAULT_QUANTITY))
            interval = int(bot.get("bot_wyckoff_paper_interval_sec", DEFAULT_INTERVAL))
            mode = 'paper'

            result = ExecutorService(db).execute_open_candidates(limit=limit, quantity=quantity, mode=mode)
            db.commit()
            print(f"Wyckoff/SMC paper tick ({mode}): {result}", flush=True)

        except Exception as exc:
            print(f"Wyckoff/SMC paper error: {exc}", flush=True)
            interval = 30
        finally:
            try:
                db.close()
            except Exception:
                pass

        time.sleep(interval)

#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.session import SessionLocal
from app.services.momentum_backtest_service import MomentumBacktestService

POLL_SECONDS = int(os.getenv("MOMENTUM_BACKTEST_WORKER_POLL", "10"))


if __name__ == "__main__":
    print("Momentum backtest worker started", flush=True)
    while True:
        db = SessionLocal()
        try:
            result = MomentumBacktestService(db).process_next_queued()
            if result:
                print(f"Momentum backtest completed: {result}", flush=True)
            else:
                print("Momentum backtest worker idle", flush=True)
        except Exception as exc:
            print(f"Momentum backtest worker error: {exc}", flush=True)
        finally:
            try:
                db.close()
            except Exception:
                pass
        time.sleep(POLL_SECONDS)

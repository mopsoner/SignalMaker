#!/usr/bin/env python3
"""
Scheduler worker — runs lightweight periodic orchestration.
It currently performs live position reconciliation when enabled.
"""
import os
import sys
import time
import logging

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.session import SessionLocal
from app.services.executor_service import ExecutorService
from app.services.runtime_settings import load_runtime_settings

DEFAULT_INTERVAL = 30
logger = logging.getLogger(__name__)


def run_scheduler_tick(session_factory=SessionLocal) -> int:
    """Run one scheduler tick with a newly-created, bounded session."""
    interval = DEFAULT_INTERVAL
    db = session_factory()
    logger.debug("scheduler DB session opened")
    try:
        runtime = load_runtime_settings(db)
        bot = runtime.get('bot', {})
        live_cfg = runtime.get('live', {})
        interval = int(bot.get('bot_scheduler_interval_sec', DEFAULT_INTERVAL))
        if bot.get('bot_scheduler_enabled', True) and live_cfg.get('live_reconcile_enabled', True):
            result = ExecutorService(db).reconcile_live_positions()
            db.commit()
            print(f'Scheduler reconcile tick: {result}', flush=True)
        elif not bot.get('bot_scheduler_enabled', True):
            interval = 30
            print('Scheduler disabled', flush=True)
        else:
            db.rollback()
            print('Scheduler tick: live reconciliation disabled', flush=True)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
        logger.debug("scheduler DB session closed")
    return interval

if __name__ == "__main__":
    print("Scheduler worker started", flush=True)
    while True:
        try:
            interval = run_scheduler_tick()
        except Exception as exc:
            print(f'Scheduler error: {exc}', flush=True)
            interval = DEFAULT_INTERVAL

        time.sleep(interval)

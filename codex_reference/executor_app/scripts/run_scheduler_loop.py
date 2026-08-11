#!/usr/bin/env python3
"""
Scheduler worker — runs lightweight periodic orchestration.
It currently performs live position reconciliation when enabled.
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
from app.services.runtime_settings import DEFAULT_SETTINGS, load_runtime_settings

TECHNICAL_FALLBACK_INTERVAL = 30


def _settings_fallback(section: str, key: str, technical_fallback):
    """Resolve a fallback from settings/DEFAULT_SETTINGS, then technical constants."""
    value = getattr(settings, key, None)
    if value is not None:
        return value

    value = DEFAULT_SETTINGS.get(section, {}).get(key)
    if value is not None:
        return value

    print(
        f"Settings fallback unavailable for {section}.{key}; using technical fallback {technical_fallback!r}",
        flush=True,
    )
    return technical_fallback


if __name__ == "__main__":
    print("Scheduler worker started", flush=True)
    while True:
        interval = _settings_fallback("bot", "bot_scheduler_interval_sec", TECHNICAL_FALLBACK_INTERVAL)
        db = SessionLocal()
        try:
            runtime = load_runtime_settings(db)
            bot = runtime.get('bot', {})
            live_cfg = runtime.get('live', {})

            if not bot.get('bot_scheduler_enabled', True):
                print('Scheduler disabled — sleeping 30s', flush=True)
                time.sleep(30)
                continue

            interval_fallback = _settings_fallback('bot', 'bot_scheduler_interval_sec', TECHNICAL_FALLBACK_INTERVAL)
            interval = int(bot.get('bot_scheduler_interval_sec', interval_fallback))
            if live_cfg.get('live_reconcile_enabled', True):
                result = ExecutorService(db).reconcile_live_positions()
                print(f'Scheduler reconcile tick: {result}', flush=True)
            else:
                print('Scheduler tick: live reconciliation disabled', flush=True)
        except Exception as exc:
            print(f'Scheduler error: {exc}', flush=True)
            interval = int(_settings_fallback("bot", "bot_scheduler_interval_sec", TECHNICAL_FALLBACK_INTERVAL))
        finally:
            try:
                db.close()
            except Exception:
                pass

        time.sleep(interval)

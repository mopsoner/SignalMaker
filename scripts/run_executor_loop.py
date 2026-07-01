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

from app.core.config import settings  # noqa: E402
from app.db.session import SessionLocal  # noqa: E402
from app.services.executor_service import ExecutorService  # noqa: E402
from app.services.runtime_settings import DEFAULT_SETTINGS, load_runtime_settings  # noqa: E402

TECHNICAL_FALLBACK_INTERVAL = 30
TECHNICAL_FALLBACK_LIMIT = 10
TECHNICAL_FALLBACK_QUANTITY = 1.0


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
    print("Executor worker started", flush=True)
    while True:
        db = SessionLocal()
        try:
            runtime = load_runtime_settings(db)
            bot = runtime.get("bot", {})
            live_cfg = runtime.get("live", {})
            momentum_cfg = runtime.get("momentum", {})

            if not bot.get("bot_executor_enabled", True):
                print("Executor disabled — sleeping 30s", flush=True)
                time.sleep(30)
                continue

            limit_fallback = _settings_fallback("bot", "bot_executor_limit", TECHNICAL_FALLBACK_LIMIT)
            quantity_fallback = _settings_fallback("bot", "bot_executor_quantity", TECHNICAL_FALLBACK_QUANTITY)
            interval_fallback = _settings_fallback("bot", "bot_executor_interval_sec", TECHNICAL_FALLBACK_INTERVAL)
            limit = int(bot.get("bot_executor_limit", limit_fallback))
            quantity = float(bot.get("bot_executor_quantity", quantity_fallback))
            interval = int(bot.get("bot_executor_interval_sec", interval_fallback))
            mode = 'live' if live_cfg.get('live_trading_enabled', settings.live_trading_enabled) else 'paper'

            sync_momentum_first = bool(momentum_cfg.get('momentum_candidates_sync_enabled', settings.momentum_candidates_sync_enabled))
            result = ExecutorService(db).execute_open_candidates(limit=limit, quantity=quantity, mode=mode, sync_momentum_first=sync_momentum_first)
            print(f"Executor tick ({mode}): {result}", flush=True)

        except Exception as exc:
            print(f"Executor error: {exc}", flush=True)
            interval = int(_settings_fallback("bot", "bot_executor_interval_sec", TECHNICAL_FALLBACK_INTERVAL))
        finally:
            try:
                db.close()
            except Exception:
                pass

        time.sleep(interval)

#!/usr/bin/env python3
"""
Executor worker — runs classic and momentum executor flows on a
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


def _runtime_bool(value, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off", ""}:
            return False
    if value is None:
        return default
    return bool(value)


if __name__ == "__main__":
    print("Executor worker started", flush=True)
    while True:
        db = SessionLocal()
        try:
            runtime = load_runtime_settings(db)
            bot = runtime.get("bot", {})
            live_cfg = runtime.get("live", {})
<<<<<<< codex/supprimer-lectures-momentum_candidates_sync_enabled
            executor_enabled = _runtime_bool(bot.get("bot_executor_enabled"), True)
            momentum_executor_enabled = _runtime_bool(bot.get("bot_executor_momentum_enabled"), False)
=======
>>>>>>> raspberry/executor-app

            if not executor_enabled and not momentum_executor_enabled:
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

<<<<<<< codex/supprimer-lectures-momentum_candidates_sync_enabled
            executor = ExecutorService(db)
            if momentum_executor_enabled:
                momentum_result = executor.execute_momentum_decision(quantity=quantity, mode=mode)
                print(
                    "Momentum executor decision: "
                    f"decision_action={momentum_result.get('decision_action')} "
                    f"symbol={momentum_result.get('symbol')} "
                    f"target_symbol={momentum_result.get('target_symbol')} "
                    f"status={momentum_result.get('status')} "
                    f"order_ids={momentum_result.get('order_ids')} "
                    f"fill_ids={momentum_result.get('fill_ids')} "
                    f"reason={momentum_result.get('reason')}",
                    flush=True,
                )

            if executor_enabled:
                result = executor.execute_open_candidates(limit=limit, quantity=quantity, mode=mode)
                print(f"Executor tick ({mode}): {result}", flush=True)
            else:
                print("Classic executor disabled by bot_executor_enabled=False", flush=True)
=======
            result = ExecutorService(db).execute_open_candidates(limit=limit, quantity=quantity, mode=mode)
            print(f"Executor tick ({mode}): {result}", flush=True)
>>>>>>> raspberry/executor-app

        except Exception as exc:
            print(f"Executor error: {exc}", flush=True)
            interval = int(_settings_fallback("bot", "bot_executor_interval_sec", TECHNICAL_FALLBACK_INTERVAL))
        finally:
            try:
                db.close()
            except Exception:
                pass

        time.sleep(interval)

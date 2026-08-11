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

from app.core.config import settings
from app.db.session import SessionLocal
from app.services.pipeline_service import PipelineService
from app.services.runtime_settings import DEFAULT_SETTINGS, load_runtime_settings

TECHNICAL_FALLBACK_INTERVAL = 60
TECHNICAL_FALLBACK_LIMIT = 25


def _settings_fallback(section: str, key: str, technical_fallback: Any) -> Any:
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


def _resolve_pipeline_limit(runtime: dict[str, dict[str, Any]]) -> int:
    """Resolve the pipeline symbol limit from canonical or legacy runtime settings."""
    market_data = runtime.get("market_data", {})
    kraken = runtime.get("kraken", {})
    fallback = _settings_fallback("market_data", "kraken_max_symbols", TECHNICAL_FALLBACK_LIMIT)
    return int(market_data.get("kraken_max_symbols", kraken.get("kraken_max_symbols", fallback)))


def _run_pipeline_tick(db: Any) -> int:
    """Run one pipeline worker tick and return the next sleep interval in seconds."""
    runtime = load_runtime_settings(db)
    bot = runtime.get("bot", {})

    if not bot.get("bot_pipeline_enabled", True):
        print("Pipeline disabled — sleeping 30s", flush=True)
        return 30

    limit = _resolve_pipeline_limit(runtime)
    interval_fallback = _settings_fallback("bot", "bot_pipeline_interval_sec", TECHNICAL_FALLBACK_INTERVAL)
    interval = int(bot.get("bot_pipeline_interval_sec", interval_fallback))

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
            interval = int(_settings_fallback("bot", "bot_pipeline_interval_sec", TECHNICAL_FALLBACK_INTERVAL))
        finally:
            try:
                db.close()
            except Exception:
                pass

        time.sleep(interval)

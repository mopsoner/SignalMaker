#!/usr/bin/env python3
"""
Momentum engine worker — runs the dedicated paper momentum rotation engine in background.
Reads runtime settings from DB at each tick, like the other SignalMaker workers.
"""
from __future__ import annotations

import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.session import SessionLocal
from app.services.momentum_engine_service import MomentumEngineService
from app.services.runtime_settings import load_runtime_settings

DEFAULT_INTERVAL = 300
DEFAULT_CADENCE_HOURS = 1
DEFAULT_STARTING_CAPITAL = 1000.0
DEFAULT_MIN_SCORE = 0.0


def validated_cadence_hours(value: object) -> int:
    try:
        cadence_hours = int(value)
    except (TypeError, ValueError):
        return DEFAULT_CADENCE_HOURS
    return cadence_hours if cadence_hours >= 1 else DEFAULT_CADENCE_HOURS


if __name__ == "__main__":
    print("Momentum engine worker started", flush=True)
    while True:
        db = SessionLocal()
        interval = DEFAULT_INTERVAL
        try:
            runtime = load_runtime_settings(db)
            bot = runtime.get("bot", {})
            momentum = runtime.get("momentum", {})

            cadence_hours = validated_cadence_hours(
                momentum.get("momentum_engine_cadence_hours", DEFAULT_CADENCE_HOURS)
            )
            enabled = bot.get("bot_momentum_engine_enabled", momentum.get("momentum_engine_enabled", True))
            if not enabled:
                print(f"Momentum engine disabled — cadence_hours={cadence_hours} sleeping 30s", flush=True)
                time.sleep(30)
                continue

            interval = int(bot.get("bot_momentum_engine_interval_sec", momentum.get("momentum_engine_interval_sec", DEFAULT_INTERVAL)))
            starting_capital = float(momentum.get("momentum_engine_starting_capital", DEFAULT_STARTING_CAPITAL))
            min_score = float(momentum.get("momentum_engine_min_score", DEFAULT_MIN_SCORE))

            result = MomentumEngineService(db).run_once(
                force=False,
                cadence_hours=cadence_hours,
                starting_capital=starting_capital,
                min_momentum_score=min_score,
            )
            position = result.get("open_position") or {}
            best_asset = result.get("best_asset") or {}
            print(
                "Momentum engine tick: "
                f"cadence_hours={cadence_hours} "
                f"due={result.get('due_now')} "
                f"due_reason={result.get('due_reason') or 'none'} "
                f"cadence_due={result.get('cadence_due')} "
                f"event_due={result.get('event_due')} "
                f"equity={result.get('equity')} "
                f"pnl={result.get('total_pnl')} "
                f"position={position.get('symbol', 'cash')} "
                f"best={best_asset.get('symbol', 'none')} "
                f"recommendation={result.get('recommendation')}",
                flush=True,
            )

        except Exception as exc:
            print(f"Momentum engine error: {exc}", flush=True)
            interval = 30
        finally:
            try:
                db.close()
            except Exception:
                pass

        time.sleep(max(5, interval))

#!/usr/bin/env python3
"""Independent Kraken account profit monitor; disabled unless explicitly enabled."""
import argparse
import json
import logging
import time

from app.core.config import settings
from app.db.session import SessionLocal
from app.services.wyckoff_goal_service import WyckoffGoalService
from scripts.worker_runtime import heartbeat

logger = logging.getLogger(__name__)


def run_once(session_factory=SessionLocal):
    if not settings.kraken_profit_worker_enabled:
        return {"enabled": False, "reason": "KRAKEN_PROFIT_WORKER_ENABLED=false"}
    with session_factory() as db:
        result = WyckoffGoalService(db).run()
        db.commit()
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if not settings.kraken_profit_worker_enabled:
        heartbeat("kraken_profit", state="disabled")
        print(json.dumps({"enabled": False, "reason": "KRAKEN_PROFIT_WORKER_ENABLED=false"}), flush=True)
        return
    while True:
        try:
            result = run_once()
            heartbeat("kraken_profit", state="running" if result.get("enabled") else "disabled")
            print(json.dumps(result, default=str), flush=True)
        except Exception as exc:
            logger.exception("Kraken profit tick failed")
            heartbeat("kraken_profit", state="error", error=str(exc))
            if args.once:
                raise
        if args.once:
            return
        time.sleep(settings.kraken_profit_worker_interval_seconds)


if __name__ == "__main__":
    main()

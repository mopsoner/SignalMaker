import os
import time

from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.position_sync_v2 import sync_open_positions

logger = setup_logging("raspberry-order-monitor")


def run_loop() -> None:
    seconds = int(os.getenv("ORDER_MONITOR_SECONDS", "20") or "20")
    logger.info("order monitor loop started seconds=%s", seconds)
    while True:
        try:
            sync_open_positions()
        except Exception as exc:
            logger.error("order monitor loop error=%s", str(exc))
        time.sleep(seconds)


if __name__ == "__main__":
    run_loop()

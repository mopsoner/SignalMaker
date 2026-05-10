import os
import threading

from raspberry_executor.candle_auto_feed import run_loop as candle_feed_loop
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ensure_env
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.order_monitor_loop import run_loop as order_monitor_loop
from raspberry_executor.spot_executor_v2 import main as executor_main
from raspberry_executor.web_dashboard import run_web

logger = setup_logging("raspberry-executor")


def main() -> None:
    ensure_env()
    load_settings()
    host = os.getenv("WEB_HOST", "0.0.0.0")
    port = int(os.getenv("WEB_PORT", "8090"))

    threading.Thread(target=run_web, kwargs={"host": host, "port": port}, daemon=True).start()
    logger.info("local 360 dashboard started http://%s:%s", host, port)

    threading.Thread(target=candle_feed_loop, daemon=True).start()
    logger.info("candle feed thread started for SignalMaker live TFs")

    threading.Thread(target=order_monitor_loop, daemon=True).start()
    logger.info("order monitor thread started for Binance position sync")

    executor_main()


if __name__ == "__main__":
    main()

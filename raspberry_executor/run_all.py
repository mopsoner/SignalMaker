import os
import threading

from raspberry_executor.candle_auto_feed import run_loop as candle_feed_loop
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ensure_env
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.main_spot_oco import main as executor_main
from raspberry_executor.web_local import run_web

logger = setup_logging("raspberry-executor")


def main() -> None:
    ensure_env()
    settings = load_settings()
    host = os.getenv("WEB_HOST", "0.0.0.0")
    port = int(os.getenv("WEB_PORT", "8090"))

    web_thread = threading.Thread(target=run_web, kwargs={"host": host, "port": port}, daemon=True)
    web_thread.start()
    logger.info("local web UI started http://%s:%s", host, port)

    candle_thread = threading.Thread(target=candle_feed_loop, daemon=True)
    candle_thread.start()
    logger.info("candle feed thread started for SignalMaker live TFs")

    executor_main()


if __name__ == "__main__":
    main()

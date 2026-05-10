import os
import threading

from raspberry_executor.candle_auto_feed import run_loop as candle_feed_loop
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ensure_env
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.margin_settings import margin_enabled
from raspberry_executor.order_monitor_loop import run_loop as order_monitor_loop
from raspberry_executor.spot_executor_v2 import main as spot_executor_main
from raspberry_executor.wallet_position_bootstrap import bootstrap_wallet_positions
from raspberry_executor.web_dashboard_candidates import run_web

logger = setup_logging("raspberry-executor")


def executor_main() -> None:
    if margin_enabled():
        from raspberry_executor.margin_executor import main as margin_executor_main
        logger.warning("MARGIN MODE ENABLED: starting margin executor")
        margin_executor_main()
        return
    logger.info("spot mode enabled: starting spot executor")
    spot_executor_main()


def main() -> None:
    ensure_env()
    load_settings()
    host = os.getenv("WEB_HOST", "0.0.0.0")
    port = int(os.getenv("WEB_PORT", "8090"))

    try:
        summary = bootstrap_wallet_positions()
        logger.info("wallet bootstrap startup=%s", summary)
    except Exception as exc:
        logger.error("wallet bootstrap startup error=%s", str(exc))

    threading.Thread(target=run_web, kwargs={"host": host, "port": port}, daemon=True).start()
    logger.info("local 360 dashboard v2 started http://%s:%s", host, port)

    threading.Thread(target=candle_feed_loop, daemon=True).start()
    logger.info("candle feed thread started for SignalMaker live TFs")

    threading.Thread(target=order_monitor_loop, daemon=True).start()
    logger.info("order monitor thread started for Binance position sync")

    executor_main()


if __name__ == "__main__":
    main()

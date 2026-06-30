import os
import threading

from raspberry_executor.candidate_status_sync import run_loop as candidate_status_sync_loop
from raspberry_executor.candle_auto_feed import run_loop as candle_feed_loop
from raspberry_executor.candle_backfill_4h import run_loop as candle_backfill_4h_loop
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ensure_env
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.margin_settings import execution_mode, margin_enabled
from raspberry_executor.momentum_decision_feed import run_loop as momentum_decision_loop
from raspberry_executor.order_monitor_loop import run_loop as order_monitor_loop
from raspberry_executor.settings_bootstrap import bootstrap_settings
from raspberry_executor.spot_executor_v2 import main as spot_executor_main
from raspberry_executor.wallet_position_bootstrap import bootstrap_wallet_positions
from raspberry_executor.web_dashboard_margin import run_web

logger = setup_logging("raspberry-executor")


def executor_main() -> None:
    mode = execution_mode()
    if margin_enabled():
        from raspberry_executor.margin_executor import main as margin_executor_main
        logger.warning("execution mode=%s primary=margin fallback=spot", mode)
        margin_executor_main()
        return
    logger.info("execution mode=spot primary=spot")
    spot_executor_main()


def main() -> None:
    ensure_env()
    try:
        settings_summary = bootstrap_settings()
        logger.info("settings bootstrap startup=%s", settings_summary)
    except Exception as exc:
        logger.error("settings bootstrap startup error=%s", str(exc))
    settings = load_settings()
    host = os.getenv("WEB_HOST", "0.0.0.0")
    port = int(os.getenv("WEB_PORT", "8090"))

    try:
        summary = bootstrap_wallet_positions()
        logger.info("wallet bootstrap startup=%s", summary)
    except Exception as exc:
        logger.error("wallet bootstrap startup error=%s", str(exc))

    threading.Thread(target=run_web, kwargs={"host": host, "port": port}, daemon=True).start()
    logger.info("local 360 dashboard started http://%s:%s execution_mode=%s", host, port, execution_mode())

    threading.Thread(target=candle_feed_loop, daemon=True).start()
    logger.info("candle feed thread started for SignalMaker live TFs")

    threading.Thread(target=candle_backfill_4h_loop, daemon=True).start()
    logger.info("optional 4h backfill thread started")


    threading.Thread(target=momentum_decision_loop, daemon=True).start()
    logger.info("momentum decision thread started")

    threading.Thread(target=order_monitor_loop, daemon=True).start()
    logger.info("order monitor thread started for configured exchange position sync")

    threading.Thread(target=candidate_status_sync_loop, daemon=True).start()
    logger.info("candidate status sync thread started for take-profit protected positions")

    executor_main()


if __name__ == "__main__":
    main()

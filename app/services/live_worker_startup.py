"""Start essential workers and configured live workers through the existing supervisor."""
import logging

from app.services.execution.live_configuration import assert_wyckoff_live_configuration
from app.services.worker_control_service import WorkerControlService

logger = logging.getLogger(__name__)

ESSENTIAL_WORKERS = (
    "pipeline", "wyckoff_paper", "momentum_paper", "scheduler",
    "kraken_candle_feed", "ibkr_ingestion", "stock_etf_analysis",
)


def start_workers(configuration, controller=None):
    workers = controller or WorkerControlService()
    started = []
    for name in ESSENTIAL_WORKERS:
        try:
            result = workers.start(name)
            if result.get("action") == "started" and workers.supervisor == "local":
                started.append(name)
        except Exception:
            logger.exception("Automatic startup of essential worker %s failed", name)
    _, live_started = start_live_workers(configuration, workers)
    return workers, started + live_started


def start_live_workers(configuration, controller=None):
    workers = controller or WorkerControlService()
    started = []
    for name, enabled in (("wyckoff_live", configuration.wyckoff_live_enabled),
                          ("kraken_profit", configuration.kraken_profit_worker_enabled),
                          ("momentum_live", configuration.momentum_live_enabled)):
        try:
            if not enabled:
                workers.stop(name)
                continue
            if name == "wyckoff_live":
                assert_wyckoff_live_configuration(configuration)
            elif (not configuration.kraken_execution_enabled or configuration.kraken_dry_run
                  or not configuration.kraken_api_key or not configuration.kraken_secret_key):
                raise RuntimeError("Live worker requires enabled real Kraken execution and API credentials")
            if name == "momentum_live":
                mode = configuration.momentum_live_mode.lower()
                if mode not in {"spot", "margin"} or (mode == "margin" and not configuration.kraken_margin_execution_enabled):
                    raise RuntimeError("Momentum live requires a valid mode and enabled margin execution for margin mode")
            result = workers.start(name)
            if result.get("action") == "started" and workers.supervisor == "local":
                started.append(name)
        except Exception:
            # One unavailable worker must not prevent the API or the other worker starting.
            logger.exception("Automatic startup of %s failed; inspect its configuration and logs", name)
    return workers, started


def stop_started_workers(workers, started):
    for name in started:
        try:
            workers.stop(name)
        except Exception:
            logger.exception("Could not stop API-owned worker %s", name)

"""Start the two configured live workers through the existing worker supervisor."""
import logging

from app.services.execution.live_configuration import assert_wyckoff_live_configuration
from app.services.worker_control_service import WorkerControlService

logger = logging.getLogger(__name__)


def start_live_workers(configuration, controller=None):
    workers = controller or WorkerControlService()
    started = []
    for name, enabled in (("wyckoff_live", configuration.wyckoff_live_enabled),
                          ("kraken_profit", configuration.kraken_profit_worker_enabled)):
        try:
            if not enabled:
                workers.stop(name)
                continue
            if name == "wyckoff_live":
                assert_wyckoff_live_configuration(configuration)
            elif (not configuration.kraken_execution_enabled or configuration.kraken_dry_run
                  or not configuration.kraken_api_key or not configuration.kraken_secret_key):
                raise RuntimeError("Profit worker requires enabled real Kraken execution and API credentials")
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

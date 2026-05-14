import os
import time

from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.signalmaker_client import SignalMakerClient
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-candidate-status-sync")


def sync_interval_seconds() -> int:
    try:
        return max(10, int(os.getenv("CANDIDATE_STATUS_SYNC_SECONDS", "30") or "30"))
    except Exception:
        return 30


def _protected(position: dict) -> bool:
    return bool(position.get("tp_order_id") and position.get("sl_order_id"))


def _already_marked(position: dict) -> bool:
    return str(position.get("remote_candidate_status") or "").lower() == "executed"


def sync_executed_candidates() -> dict:
    settings = load_settings()
    client = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    state = StateStore()
    checked = protected = marked = skipped = errors = 0

    for candidate_id, position in list(state.open_positions().items()):
        checked += 1
        symbol = str(position.get("execution_symbol") or position.get("signal_symbol") or "").upper()
        if not _protected(position):
            skipped += 1
            continue
        protected += 1
        if _already_marked(position):
            skipped += 1
            continue
        try:
            response = client.mark_candidate_executed(candidate_id)
            state.update_open_position(candidate_id, {
                "remote_candidate_status": "executed",
                "remote_candidate_executed_source": "candidate_status_sync",
                "remote_candidate_executed_response": response,
            }, event_type="candidate_marked_executed")
            logger.info("candidate marked executed after OCO protection candidate=%s symbol=%s", candidate_id, symbol)
            marked += 1
        except Exception as exc:
            errors += 1
            state.update_open_position(candidate_id, {
                "remote_candidate_mark_error": str(exc),
                "remote_candidate_mark_error_source": "candidate_status_sync",
            }, event_type="candidate_mark_executed_failed")
            logger.warning("candidate executed mark failed candidate=%s symbol=%s error=%s", candidate_id, symbol, str(exc))

    summary = {"checked": checked, "protected": protected, "marked": marked, "skipped": skipped, "errors": errors}
    if marked or errors:
        logger.info("candidate status sync summary=%s", summary)
    return summary


def run_loop() -> None:
    while True:
        try:
            sync_executed_candidates()
        except Exception as exc:
            logger.error("candidate status sync loop error=%s", str(exc))
        time.sleep(sync_interval_seconds())


if __name__ == "__main__":
    print(sync_executed_candidates())

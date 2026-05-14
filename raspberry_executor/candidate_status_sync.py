import os
import time

from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-candidate-status-sync")


def sync_interval_seconds() -> int:
    try:
        return max(10, int(os.getenv("CANDIDATE_STATUS_SYNC_SECONDS", "30") or "30"))
    except Exception:
        return 30


def _protected(position: dict) -> bool:
    return bool(position.get("tp_order_id") and position.get("sl_order_id"))


def _already_marked_local(state: StateStore, candidate_id: str, position: dict) -> bool:
    if state.already_executed(candidate_id):
        return True
    return str(position.get("local_candidate_status") or "").lower() == "executed"


def sync_executed_candidates() -> dict:
    """Mark protected candidates as executed in the local Raspberry SQLite DB only.

    This does not call the remote SignalMaker server. The remote candidate can
    remain open for history/back-office visibility, while the Raspberry local DB
    prevents re-execution once a local position has TP and SL/OCO protection.
    """
    state = StateStore()
    checked = protected = marked = skipped = errors = 0

    for candidate_id, position in list(state.open_positions().items()):
        checked += 1
        symbol = str(position.get("execution_symbol") or position.get("signal_symbol") or "").upper()
        if not _protected(position):
            skipped += 1
            continue
        protected += 1
        if _already_marked_local(state, candidate_id, position):
            skipped += 1
            continue
        try:
            state.mark_executed(candidate_id)
            state.update_open_position(candidate_id, {
                "local_candidate_status": "executed",
                "local_candidate_executed_source": "candidate_status_sync",
                "local_candidate_executed_reason": "position_has_tp_and_sl",
            }, event_type="candidate_marked_executed_local")
            logger.info("candidate marked executed locally after OCO protection candidate=%s symbol=%s", candidate_id, symbol)
            marked += 1
        except Exception as exc:
            errors += 1
            state.update_open_position(candidate_id, {
                "local_candidate_mark_error": str(exc),
                "local_candidate_mark_error_source": "candidate_status_sync",
            }, event_type="candidate_mark_executed_local_failed")
            logger.warning("candidate local executed mark failed candidate=%s symbol=%s error=%s", candidate_id, symbol, str(exc))

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

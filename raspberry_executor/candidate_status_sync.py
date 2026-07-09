import os
import time

from raspberry_executor.local_candidate_store import mark_candidate_executed
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-candidate-status-sync")


def sync_interval_seconds() -> int:
    try:
        return max(10, int(os.getenv("CANDIDATE_STATUS_SYNC_SECONDS", "30") or "30"))
    except Exception:
        return 30


CONFIRMED_TP_STATUSES = {"NEW", "OPEN", "PARTIALLY_FILLED", "ACCEPTED", "PENDING", "PLACED"}


def _truthy(value) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (dict, list, tuple, set)):
        return bool(value)
    return bool(value)


def _positive_quantity(value) -> bool:
    try:
        return float(value or 0) > 0
    except Exception:
        return _truthy(value)


def _tp_status(position: dict) -> str:
    status = position.get("tp_exchange_status")
    if not status and isinstance(position.get("tp_payload"), dict):
        payload = position.get("tp_payload") or {}
        status = payload.get("status") or payload.get("state") or payload.get("ordStatus")
    return str(status or "").upper()


def _tp_protection_state(position: dict) -> dict[str, bool | str]:
    tp_order_id = position.get("tp_order_id")
    tp_payload = position.get("tp_payload") if isinstance(position.get("tp_payload"), dict) else {}
    local_tp_recorded = bool(
        _truthy(tp_order_id)
        and (
            _truthy(position.get("execution_symbol"))
            or _truthy(position.get("quantity"))
            or _truthy(position.get("entry_order_id"))
            or _truthy(tp_payload)
            or _truthy(position.get("tp_exchange_status"))
        )
    )
    status = _tp_status(position)
    exchange_tp_confirmed = bool(local_tp_recorded and status in CONFIRMED_TP_STATUSES)
    position_exists_on_asset = bool(
        _truthy(position.get("execution_symbol") or position.get("signal_symbol"))
        and (_positive_quantity(position.get("quantity")) or _truthy(position.get("entry_order_id")))
    )
    return {
        "local_tp_recorded": local_tp_recorded,
        "exchange_tp_confirmed": exchange_tp_confirmed,
        "position_exists_on_asset": position_exists_on_asset,
        "tp_exchange_status": status,
    }


def _protected(position: dict) -> bool:
    state = _tp_protection_state(position)
    return bool(state["local_tp_recorded"] and state["exchange_tp_confirmed"] and state["position_exists_on_asset"])


def _already_marked_local(state: StateStore, candidate_id: str, position: dict) -> bool:
    if state.already_executed(candidate_id):
        return True
    return str(position.get("local_candidate_status") or "").lower() == "executed"


def sync_executed_candidates() -> dict:
    """Mark candidates with a confirmed take-profit order as executed in local Raspberry SQLite only."""
    state = StateStore()
    checked = protected = marked = skipped = errors = 0

    for candidate_id, position in list(state.open_positions().items()):
        checked += 1
        symbol = str(position.get("execution_symbol") or position.get("signal_symbol") or "").upper()
        protection_state = _tp_protection_state(position)
        if not _protected(position):
            if protection_state["local_tp_recorded"] and not protection_state["exchange_tp_confirmed"]:
                state.add_event(candidate_id, "candidate_tp_unconfirmed", {
                    "symbol": symbol,
                    "tp_order_id": position.get("tp_order_id"),
                    "tp_exchange_status": protection_state["tp_exchange_status"],
                    "local_tp_recorded": protection_state["local_tp_recorded"],
                    "exchange_tp_confirmed": protection_state["exchange_tp_confirmed"],
                    "position_exists_on_asset": protection_state["position_exists_on_asset"],
                    "source": "candidate_status_sync",
                })
                logger.info(
                    "candidate take-profit local record not exchange-confirmed candidate=%s symbol=%s tp_order_id=%s status=%s",
                    candidate_id,
                    symbol,
                    position.get("tp_order_id"),
                    protection_state["tp_exchange_status"],
                )
            skipped += 1
            continue
        protected += 1
        if _already_marked_local(state, candidate_id, position):
            skipped += 1
            continue
        try:
            state.mark_executed(candidate_id)
            mark_candidate_executed(candidate_id)
            remote_candidate_id = position.get("remote_candidate_id") or position.get("candidate", {}).get("remote_candidate_id")
            if remote_candidate_id:
                mark_candidate_executed(str(remote_candidate_id))
            state.update_open_position(candidate_id, {
                "local_candidate_status": "executed",
                "local_candidate_executed_source": "candidate_status_sync",
                "local_candidate_executed_reason": "position_has_confirmed_take_profit",
                "local_tp_recorded": protection_state["local_tp_recorded"],
                "exchange_tp_confirmed": protection_state["exchange_tp_confirmed"],
                "position_exists_on_asset": protection_state["position_exists_on_asset"],
            }, event_type="candidate_marked_executed_local")
            logger.info("candidate marked executed locally after take-profit placement candidate=%s symbol=%s", candidate_id, symbol)
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

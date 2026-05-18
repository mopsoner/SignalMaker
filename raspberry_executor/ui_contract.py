from typing import Any

from raspberry_executor.candidate_cursor_store import read_candidate_cursor
from raspberry_executor.candidate_view_store import candidate_status_summary, local_candidate_rows
from raspberry_executor.env_store import public_env
from raspberry_executor.position_sync_v2 import sync_open_positions
from raspberry_executor.sqlite_db import connect
from raspberry_executor.state import StateStore

CANDIDATE_LABELS = [
    "Local state",
    "Execution state",
    "Candidate",
    "Remote",
    "Symbol",
    "Side",
    "Entry",
    "Stop",
    "Target",
    "First seen",
    "Last seen",
    "Fingerprint",
]

CANDIDATE_KEYS = [
    "local_state",
    "execution_state",
    "candidate",
    "remote",
    "symbol",
    "side",
    "entry",
    "stop",
    "target",
    "first_seen",
    "last_seen",
    "fingerprint",
]

POSITION_LABELS = [
    "Status",
    "Candidate",
    "Symbol",
    "Side",
    "Qty",
    "Entry",
    "Stop",
    "Target",
    "TP",
    "TP status",
    "SL",
    "SL status",
    "Reason",
]

POSITION_KEYS = [
    "status",
    "candidate",
    "symbol",
    "side",
    "qty",
    "entry",
    "stop",
    "target",
    "tp",
    "tp_status",
    "sl",
    "sl_status",
    "reason",
]

STATUS_LABELS = {
    "DRY_RUN": "Dry run",
    "ORDER_QUOTE_AMOUNT": "Order quote",
    "QUOTE_ASSETS": "Quote assets",
    "CANDLE_FEED_ENABLED": "Candle feed enabled",
    "CANDLE_FEED_INTERVALS": "Candle feed intervals",
    "CANDLE_FEED_POLL_SECONDS": "Candle feed poll seconds",
    "CANDLE_FEED_MAX_WORKERS": "Candle feed workers",
    "SIGNALMAKER_BASE_URL": "SignalMaker URL",
    "GATEWAY_ID": "Gateway ID",
}


def _string(value: Any) -> str:
    return "" if value is None else str(value)


def _meta_value(key: str) -> str:
    try:
        with connect() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return _string(row["value"] if row else "")
    except Exception:
        return ""


def order_status(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    if payload.get("sync_error"):
        return "sync_error"
    return _string(payload.get("status"))


def candidate_row(row: dict[str, Any]) -> dict[str, str]:
    return {
        "local_state": _string(row.get("local_status")),
        "execution_state": _string(row.get("execution_state")),
        "candidate": _string(row.get("candidate_id")),
        "remote": _string(row.get("remote_candidate_id")),
        "symbol": _string(row.get("symbol")),
        "side": _string(row.get("side")),
        "entry": _string(row.get("entry_price")),
        "stop": _string(row.get("stop_price")),
        "target": _string(row.get("target_price")),
        "first_seen": _string(row.get("first_seen_at")),
        "last_seen": _string(row.get("last_seen_at")),
        "fingerprint": _string(row.get("signal_fingerprint")),
    }


def candidates_view(limit: int = 100) -> dict[str, Any]:
    rows = [candidate_row(row) for row in local_candidate_rows(limit=limit, include_executed=True)]
    return {
        "title": "SignalMaker Trade Candidates",
        "labels": CANDIDATE_LABELS,
        "keys": CANDIDATE_KEYS,
        "summary": candidate_status_summary(limit=max(limit, 500)),
        "rows": rows,
        "empty_message": "No local candidates.",
        "help": "Local SQLite candidates only. Local state stays received until the local candidate status changes; execution state shows whether executor consumed the signal.",
        "cursor": read_candidate_cursor(),
        "last_runtime_reset_at": _meta_value("local_runtime_data_reset_at"),
        "ignored_old_after_reset": _meta_value("local_candidates_ignored_old_after_reset"),
    }


def position_row(candidate_id: str, row: dict[str, Any]) -> dict[str, str]:
    return {
        "status": _string(row.get("status")),
        "candidate": _string(candidate_id),
        "symbol": _string(row.get("execution_symbol") or row.get("signal_symbol")),
        "side": _string(row.get("side")),
        "qty": _string(row.get("quantity")),
        "entry": _string(row.get("entry_price")),
        "stop": _string(row.get("stop_price")),
        "target": _string(row.get("target_price")),
        "tp": _string(row.get("tp_order_id")),
        "tp_status": order_status(row.get("binance_tp_status")),
        "sl": _string(row.get("sl_order_id")),
        "sl_status": order_status(row.get("binance_sl_status")),
        "reason": _string(row.get("close_reason")),
    }


def positions_view(limit: int = 50, sync: bool = True) -> dict[str, Any]:
    sync_result: dict[str, Any] | None = None
    sync_error = ""
    if sync:
        try:
            sync_result = sync_open_positions()
        except Exception as exc:
            sync_error = str(exc)
    state = StateStore()
    open_rows = [position_row(candidate_id, row) for candidate_id, row in state.open_positions().items()]
    closed_items = [(item.get("candidate_id", ""), item) for item in reversed(state.closed_positions()[-limit:])]
    closed_rows = [position_row(candidate_id, row) for candidate_id, row in closed_items]
    return {
        "title": "Binance Synced Positions",
        "labels": POSITION_LABELS,
        "keys": POSITION_KEYS,
        "open_rows": open_rows,
        "closed_rows": closed_rows,
        "empty_message": "No positions.",
        "sync": sync_result or {},
        "sync_error": sync_error,
    }


def status_view() -> dict[str, Any]:
    env = public_env()
    rows = []
    for key, label in STATUS_LABELS.items():
        rows.append({"key": key, "label": label, "value": _string(env.get(key))})
    rows.append({"key": "candidate_cursor", "label": "Candidate cursor", "value": _string(read_candidate_cursor())})
    rows.append({"key": "last_runtime_reset_at", "label": "Last local reset", "value": _meta_value("local_runtime_data_reset_at")})
    return {"title": "Raspberry Executor Status", "rows": rows}

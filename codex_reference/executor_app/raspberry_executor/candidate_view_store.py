from typing import Any

from raspberry_executor.local_candidate_store import init_local_candidate_store
from raspberry_executor.sqlite_db import connect, loads
from raspberry_executor.state import StateStore


def _executed_ids(state: StateStore) -> set[str]:
    with connect() as conn:
        rows = conn.execute("SELECT candidate_id FROM executed_candidates").fetchall()
        return {str(row["candidate_id"]) for row in rows}


def local_candidate_rows(limit: int = 100, include_executed: bool = True, only_received: bool = False) -> list[dict[str, Any]]:
    """Return the canonical candidate view for Web and TUI.

    local_trade_candidates.status is the UI-facing candidate lifecycle:
    - received = signal has been received and stored locally
    - executed = executor consumed it

    executed_candidates is an execution lock/dedupe table. It must not hide the
    received row in the TUI by itself, otherwise a long can disappear from the
    "received candidates" view immediately after the executor sees it.
    """
    init_local_candidate_store()
    state = StateStore()
    executed = _executed_ids(state)
    sql = "SELECT * FROM local_trade_candidates ORDER BY first_seen_at DESC LIMIT ?"
    with connect() as conn:
        rows = conn.execute(sql, (limit,)).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        payload = loads(row["payload_json"], {})
        local_id = str(row["local_candidate_id"] or "")
        remote_id = str(row["remote_candidate_id"] or "")
        local_status = str(row["status"] or "received")
        execution_consumed = local_id in executed or remote_id in executed or local_status == "executed"
        if only_received and local_status != "received":
            continue
        if local_status == "executed" and not include_executed:
            continue
        result.append({
            "candidate_id": local_id,
            "remote_candidate_id": remote_id,
            "symbol": row["symbol"] or payload.get("symbol"),
            "side": row["side"] or payload.get("side"),
            "entry_price": row["entry_price"] or payload.get("entry_price"),
            "target_price": row["target_price"] or payload.get("target_price"),
            "stop_price": row["stop_price"] or payload.get("stop_price"),
            "local_status": local_status,
            "execution_state": "consumed" if execution_consumed else "not_consumed",
            "remote_status": payload.get("status"),
            "signal_fingerprint": row["fingerprint"],
            "first_seen_at": row["first_seen_at"],
            "last_seen_at": row["last_seen_at"],
            "is_executed": execution_consumed,
            "payload": payload,
        })
    return result


def received_candidate_rows(limit: int = 100) -> list[dict[str, Any]]:
    return local_candidate_rows(limit=limit, include_executed=True, only_received=True)


def candidate_status_summary(limit: int = 500) -> dict[str, int]:
    rows = local_candidate_rows(limit=limit, include_executed=True)
    summary = {"total": len(rows), "received": 0, "executed": 0, "consumed": 0, "other": 0}
    for row in rows:
        status = str(row.get("local_status") or "received")
        if row.get("execution_state") == "consumed":
            summary["consumed"] += 1
        if status == "executed":
            summary["executed"] += 1
        elif status == "received":
            summary["received"] += 1
        else:
            summary["other"] += 1
    return summary

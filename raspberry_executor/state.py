from typing import Any

from raspberry_executor.sqlite_db import connect, dumps, init_db, loads, now_iso, upsert_position


class StateStore:
    def __init__(self, path: str = "state.json") -> None:
        self.path = path
        init_db()

    def now(self) -> str:
        return now_iso()

    def already_executed(self, candidate_id: str) -> bool:
        with connect() as conn:
            row = conn.execute("SELECT 1 FROM executed_candidates WHERE candidate_id=?", (candidate_id,)).fetchone()
            return row is not None

    def mark_executed(self, candidate_id: str) -> None:
        with connect() as conn:
            conn.execute("INSERT OR IGNORE INTO executed_candidates(candidate_id, executed_at) VALUES(?, ?)", (candidate_id, self.now()))

    def _fingerprint_key(self, fingerprint: str) -> str:
        return f"signal-fingerprint:{fingerprint}"

    def already_executed_fingerprint(self, fingerprint: str) -> bool:
        if not fingerprint:
            return False
        return self.already_executed(self._fingerprint_key(fingerprint))

    def mark_executed_fingerprint(self, fingerprint: str) -> None:
        if fingerprint:
            self.mark_executed(self._fingerprint_key(fingerprint))

    def has_open_position_for(self, symbol: str, side: str | None = None) -> bool:
        wanted_symbol = str(symbol or "").upper()
        wanted_side = str(side or "").lower()
        if not wanted_symbol:
            return False
        for _, position in self.open_positions().items():
            pos_symbol = str(position.get("execution_symbol") or position.get("signal_symbol") or "").upper()
            pos_side = str(position.get("side") or "").lower()
            if pos_symbol != wanted_symbol:
                continue
            if wanted_side and pos_side != wanted_side:
                continue
            return True
        return False

    def add_open_position(self, candidate_id: str, payload: dict[str, Any]) -> None:
        payload = {**payload, "status": "open", "opened_at": payload.get("opened_at") or self.now()}
        with connect() as conn:
            upsert_position(conn, candidate_id, "open", payload)
            conn.execute("INSERT INTO events(candidate_id, event_type, timestamp, payload_json) VALUES(?, ?, ?, ?)", (candidate_id, "position_opened", self.now(), dumps(payload)))

    def update_open_position(self, candidate_id: str, updates: dict[str, Any], event_type: str | None = None) -> None:
        with connect() as conn:
            row = conn.execute("SELECT payload_json FROM positions WHERE candidate_id=? AND status='open'", (candidate_id,)).fetchone()
            if row is None:
                return
            position = loads(row["payload_json"], {})
            position.update(updates)
            position["status"] = "open"
            upsert_position(conn, candidate_id, "open", position)
            if event_type:
                conn.execute("INSERT INTO events(candidate_id, event_type, timestamp, payload_json) VALUES(?, ?, ?, ?)", (candidate_id, event_type, self.now(), dumps(updates)))

    def close_position(self, candidate_id: str, reason: str, payload: dict[str, Any] | None = None, *, record_event: bool = True) -> None:
        with connect() as conn:
            row = conn.execute("SELECT payload_json FROM positions WHERE candidate_id=? AND status='open'", (candidate_id,)).fetchone()
            if row is None:
                return
            position = loads(row["payload_json"], {})
            position = {**position, "status": "closed", "close_reason": reason, "closed_at": self.now(), "close_payload": payload or {}}
            upsert_position(conn, candidate_id, "closed", position)
            if record_event:
                conn.execute("INSERT INTO events(candidate_id, event_type, timestamp, payload_json) VALUES(?, ?, ?, ?)", (candidate_id, reason, self.now(), dumps(payload or {})))

    def remove_open_position(self, candidate_id: str) -> None:
        with connect() as conn:
            conn.execute("DELETE FROM positions WHERE candidate_id=? AND status='open'", (candidate_id,))

    def _position_from_row(self, row) -> dict[str, Any]:
        payload = loads(row["payload_json"], {})
        payload.update({
            "candidate_id": row["candidate_id"],
            "status": row["status"],
            "signal_symbol": row["signal_symbol"],
            "execution_symbol": row["execution_symbol"],
            "side": row["side"],
            "quantity": row["quantity"],
            "entry_price": row["entry_price"],
            "stop_price": row["stop_price"],
            "target_price": row["target_price"],
            "entry_order_id": row["entry_order_id"],
            "oco_order_list_id": row["oco_order_list_id"],
            "tp_order_id": row["tp_order_id"],
            "sl_order_id": row["sl_order_id"],
            "opened_at": row["opened_at"],
            "closed_at": row["closed_at"],
            "close_reason": row["close_reason"],
            "close_payload": loads(row["close_payload_json"], {}),
        })
        return payload

    def open_positions(self) -> dict[str, Any]:
        with connect() as conn:
            rows = conn.execute("SELECT * FROM positions WHERE status='open' ORDER BY opened_at DESC").fetchall()
            return {row["candidate_id"]: self._position_from_row(row) for row in rows}

    def closed_positions(self) -> list[dict[str, Any]]:
        with connect() as conn:
            rows = conn.execute("SELECT * FROM positions WHERE status='closed' ORDER BY closed_at ASC LIMIT 500").fetchall()
            return [self._position_from_row(row) for row in rows]

    def add_event(self, candidate_id: str, event_type: str, payload: dict[str, Any] | None = None, *, save: bool = True) -> None:
        with connect() as conn:
            conn.execute("INSERT INTO events(candidate_id, event_type, timestamp, payload_json) VALUES(?, ?, ?, ?)", (candidate_id, event_type, self.now(), dumps(payload or {})))

    def events(self, limit: int = 1000) -> list[dict[str, Any]]:
        # Keep the UI/API focused on the newest rows. The previous ASC + LIMIT
        # query returned the oldest 1000 events forever once the table grew past
        # that size, so Web/TUI pages appeared stale even while new events were
        # still being written. Fetch newest first, then restore chronological
        # order for callers that expect append-order semantics.
        try:
            safe_limit = max(1, int(limit))
        except Exception:
            safe_limit = 1000
        with connect() as conn:
            rows = conn.execute(
                "SELECT * FROM (SELECT * FROM events ORDER BY id DESC LIMIT ?) ORDER BY id ASC",
                (safe_limit,),
            ).fetchall()
            return [{"candidate_id": row["candidate_id"], "event_type": row["event_type"], "timestamp": row["timestamp"], "payload": loads(row["payload_json"], {})} for row in rows]

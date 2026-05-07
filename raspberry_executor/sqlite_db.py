import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DB_PATH = Path("raspberry_executor.db")
STATE_JSON_PATH = Path("state.json")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def dumps(value: Any) -> str:
    return json.dumps(value or {}, sort_keys=True, ensure_ascii=False)


def loads(value: str | None, default: Any):
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def init_db() -> None:
    with connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS executed_candidates (
                candidate_id TEXT PRIMARY KEY,
                executed_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS positions (
                candidate_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                signal_symbol TEXT,
                execution_symbol TEXT,
                side TEXT,
                quantity TEXT,
                entry_price REAL,
                stop_price REAL,
                target_price REAL,
                entry_order_id TEXT,
                oco_order_list_id TEXT,
                tp_order_id TEXT,
                sl_order_id TEXT,
                opened_at TEXT,
                closed_at TEXT,
                close_reason TEXT,
                payload_json TEXT NOT NULL DEFAULT '{}',
                close_payload_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
            CREATE INDEX IF NOT EXISTS idx_positions_opened_at ON positions(opened_at);
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_id TEXT,
                event_type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
            CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
            CREATE TABLE IF NOT EXISTS retry_queue (
                key TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                last_error_at TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_retry_symbol_interval ON retry_queue(symbol, interval);
            CREATE TABLE IF NOT EXISTS feed_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                status TEXT,
                symbol_count INTEGER DEFAULT 0,
                pushed_count INTEGER DEFAULT 0,
                skipped_count INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                retry_queue_size INTEGER DEFAULT 0,
                summary_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_feed_runs_timestamp ON feed_runs(timestamp);
            """
        )
        conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', '1')")


def migrate_state_json_once() -> dict[str, Any]:
    init_db()
    with connect() as conn:
        done = conn.execute("SELECT value FROM meta WHERE key='state_json_migrated'").fetchone()
        if done and done["value"] == "true":
            return {"status": "skipped", "reason": "already_migrated"}
        if not STATE_JSON_PATH.exists():
            conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES('state_json_migrated', 'true')")
            return {"status": "skipped", "reason": "state_json_missing"}
        try:
            data = json.loads(STATE_JSON_PATH.read_text())
        except Exception as exc:
            return {"status": "failed", "reason": "state_json_invalid", "error": str(exc)}

        executed = data.get("executed_candidates") or []
        for candidate_id in executed:
            conn.execute(
                "INSERT OR IGNORE INTO executed_candidates(candidate_id, executed_at) VALUES(?, ?)",
                (str(candidate_id), now_iso()),
            )

        open_positions = data.get("open_positions") or {}
        for candidate_id, payload in open_positions.items():
            upsert_position(conn, str(candidate_id), "open", payload or {})

        closed_positions = data.get("closed_positions") or []
        for payload in closed_positions:
            candidate_id = str(payload.get("candidate_id") or payload.get("id") or "")
            if candidate_id:
                upsert_position(conn, candidate_id, "closed", payload or {})

        events = data.get("events") or []
        for event in events:
            conn.execute(
                "INSERT INTO events(candidate_id, event_type, timestamp, payload_json) VALUES(?, ?, ?, ?)",
                (
                    event.get("candidate_id"),
                    event.get("event_type") or "unknown",
                    event.get("timestamp") or now_iso(),
                    dumps(event.get("payload") or {}),
                ),
            )
        conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES('state_json_migrated', 'true')")
        return {
            "status": "ok",
            "executed_candidates": len(executed),
            "open_positions": len(open_positions),
            "closed_positions": len(closed_positions),
            "events": len(events),
        }


def upsert_position(conn: sqlite3.Connection, candidate_id: str, status: str, payload: dict[str, Any]) -> None:
    opened_at = payload.get("opened_at") or now_iso()
    closed_at = payload.get("closed_at")
    conn.execute(
        """
        INSERT INTO positions(
            candidate_id, status, signal_symbol, execution_symbol, side, quantity, entry_price,
            stop_price, target_price, entry_order_id, oco_order_list_id, tp_order_id, sl_order_id,
            opened_at, closed_at, close_reason, payload_json, close_payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_id) DO UPDATE SET
            status=excluded.status,
            signal_symbol=excluded.signal_symbol,
            execution_symbol=excluded.execution_symbol,
            side=excluded.side,
            quantity=excluded.quantity,
            entry_price=excluded.entry_price,
            stop_price=excluded.stop_price,
            target_price=excluded.target_price,
            entry_order_id=excluded.entry_order_id,
            oco_order_list_id=excluded.oco_order_list_id,
            tp_order_id=excluded.tp_order_id,
            sl_order_id=excluded.sl_order_id,
            opened_at=excluded.opened_at,
            closed_at=excluded.closed_at,
            close_reason=excluded.close_reason,
            payload_json=excluded.payload_json,
            close_payload_json=excluded.close_payload_json
        """,
        (
            candidate_id,
            status,
            payload.get("signal_symbol"),
            payload.get("execution_symbol"),
            payload.get("side"),
            str(payload.get("quantity")) if payload.get("quantity") is not None else None,
            payload.get("entry_price"),
            payload.get("stop_price"),
            payload.get("target_price"),
            str(payload.get("entry_order_id")) if payload.get("entry_order_id") is not None else None,
            str(payload.get("oco_order_list_id")) if payload.get("oco_order_list_id") is not None else None,
            str(payload.get("tp_order_id")) if payload.get("tp_order_id") is not None else None,
            str(payload.get("sl_order_id")) if payload.get("sl_order_id") is not None else None,
            opened_at,
            closed_at,
            payload.get("close_reason"),
            dumps(payload),
            dumps(payload.get("close_payload") or {}),
        ),
    )


if __name__ == "__main__":
    print(json.dumps(migrate_state_json_once(), indent=2))

from typing import Any

from raspberry_executor.sqlite_db import connect, dumps, init_db, loads, now_iso


def init_pending_trade_queue() -> None:
    init_db()
    with connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS pending_trade_queue (
                candidate_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                reason TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                candidate_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_pending_trade_queue_updated_at ON pending_trade_queue(updated_at);
            """
        )


def add_pending(candidate: dict[str, Any], reason: str) -> None:
    init_pending_trade_queue()
    now = now_iso()
    candidate_id = str(candidate.get("candidate_id") or "")
    if not candidate_id:
        return
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO pending_trade_queue(candidate_id, symbol, side, reason, attempts, created_at, updated_at, candidate_json)
            VALUES(?, ?, ?, ?, 0, ?, ?, ?)
            ON CONFLICT(candidate_id) DO UPDATE SET
                reason=excluded.reason,
                updated_at=excluded.updated_at,
                candidate_json=excluded.candidate_json
            """,
            (
                candidate_id,
                str(candidate.get("symbol") or "").upper(),
                str(candidate.get("side") or "").lower(),
                reason,
                now,
                now,
                dumps(candidate),
            ),
        )


def remove_pending(candidate_id: str) -> None:
    init_pending_trade_queue()
    with connect() as conn:
        conn.execute("DELETE FROM pending_trade_queue WHERE candidate_id=?", (candidate_id,))


def bump_pending(candidate_id: str, reason: str) -> None:
    init_pending_trade_queue()
    with connect() as conn:
        conn.execute(
            "UPDATE pending_trade_queue SET attempts=attempts+1, reason=?, updated_at=? WHERE candidate_id=?",
            (reason, now_iso(), candidate_id),
        )


def list_pending(limit: int = 50) -> list[dict[str, Any]]:
    init_pending_trade_queue()
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM pending_trade_queue ORDER BY updated_at ASC LIMIT ?",
            (int(limit),),
        ).fetchall()
        items = []
        for row in rows:
            candidate = loads(row["candidate_json"], {})
            candidate.setdefault("candidate_id", row["candidate_id"])
            candidate.setdefault("symbol", row["symbol"])
            candidate.setdefault("side", row["side"])
            items.append({
                "candidate_id": row["candidate_id"],
                "symbol": row["symbol"],
                "side": row["side"],
                "reason": row["reason"],
                "attempts": row["attempts"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "candidate": candidate,
            })
        return items

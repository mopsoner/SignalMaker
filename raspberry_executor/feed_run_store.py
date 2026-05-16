from typing import Any

from raspberry_executor.sqlite_db import connect, dumps, init_db, loads, now_iso


def record_feed_run(summary: dict[str, Any]) -> None:
    init_db()
    pushed = summary.get("pushed") or []
    skipped = summary.get("skipped") or []
    errors = summary.get("errors") or []
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO feed_runs(
                timestamp, status, symbol_count, pushed_count, skipped_count,
                error_count, retry_queue_size, summary_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now_iso(),
                str(summary.get("status") or "unknown"),
                int(summary.get("symbol_count") or 0),
                len(pushed) if isinstance(pushed, list) else 0,
                len(skipped) if isinstance(skipped, list) else 0,
                len(errors) if isinstance(errors, list) else 0,
                int(summary.get("retry_queue_size") or 0),
                dumps(summary),
            ),
        )


def latest_feed_runs(limit: int = 50) -> list[dict[str, Any]]:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM feed_runs ORDER BY timestamp DESC LIMIT ?",
            (limit,),
        ).fetchall()
        result = []
        for row in rows:
            summary = loads(row["summary_json"], {})
            result.append({
                "id": row["id"],
                "timestamp": row["timestamp"],
                "status": row["status"],
                "symbol_count": row["symbol_count"],
                "pushed_count": row["pushed_count"],
                "skipped_count": row["skipped_count"],
                "error_count": row["error_count"],
                "retry_queue_size": row["retry_queue_size"],
                "summary": summary,
            })
        return result


def latest_feed_run() -> dict[str, Any] | None:
    rows = latest_feed_runs(limit=1)
    return rows[0] if rows else None

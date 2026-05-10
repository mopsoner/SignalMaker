from raspberry_executor.sqlite_db import connect, init_db


def reset_positions_db() -> dict:
    init_db()
    with connect() as conn:
        counts = {}
        for table in [
            "positions",
            "executed_candidates",
            "events",
            "pending_trade_queue",
        ]:
            try:
                row = conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
                counts[table] = int(row["count"] if row else 0)
                conn.execute(f"DELETE FROM {table}")
            except Exception:
                counts[table] = None
        conn.execute("VACUUM")
    return {"status": "ok", "deleted": counts}


if __name__ == "__main__":
    print(reset_positions_db())

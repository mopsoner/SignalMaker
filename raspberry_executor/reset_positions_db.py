from raspberry_executor.sqlite_db import connect, init_db

TRACKING_TABLES = [
    "positions",
    "executed_candidates",
    "events",
    "pending_trade_queue",
]


def reset_positions_db() -> dict:
    init_db()
    counts = {}
    errors = {}

    with connect() as conn:
        for table in TRACKING_TABLES:
            try:
                row = conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
                counts[table] = int(row["count"] if row else 0)
                conn.execute(f"DELETE FROM {table}")
            except Exception as exc:
                counts[table] = None
                errors[table] = str(exc)
        conn.commit()

    # VACUUM cannot run inside the same transaction as DELETE.
    try:
        with connect() as conn:
            conn.execute("VACUUM")
    except Exception as exc:
        errors["vacuum"] = str(exc)

    return {"status": "ok" if not errors else "partial", "deleted": counts, "errors": errors}


if __name__ == "__main__":
    print(reset_positions_db())

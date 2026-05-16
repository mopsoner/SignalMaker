from raspberry_executor.sqlite_db import connect, init_db

# Local runtime settings are intentionally NOT listed here.
# The admin reset button should clear runtime/trading history, but preserve
# persistent settings such as env values, endpoints, sizing, and dashboard config.
TRACKING_TABLES = [
    "positions",
    "executed_candidates",
    "events",
    "pending_trade_queue",
    "local_trade_candidates",
    "feed_runs",
    "retry_queue",
]

PRESERVED_TABLES = ["settings", "meta"]


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
        preserved = {}
        for table in PRESERVED_TABLES:
            try:
                row = conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
                preserved[table] = int(row["count"] if row else 0)
            except Exception as exc:
                preserved[table] = None
                errors[f"preserved_{table}"] = str(exc)
        conn.commit()

    try:
        with connect() as conn:
            conn.execute("VACUUM")
    except Exception as exc:
        errors["vacuum"] = str(exc)

    return {"status": "ok" if not errors else "partial", "deleted": counts, "preserved": preserved, "errors": errors}


if __name__ == "__main__":
    print(reset_positions_db())

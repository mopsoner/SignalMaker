import asyncio

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session

from signalmaker.market_data.repository import MarketDataRepository


def test_combined_history_uses_typed_null_heartbeats():
    engine = create_engine("sqlite://")
    with Session(engine) as db:
        repo = MarketDataRepository(db)
        repo.ensure_schema()
        db.execute(text("INSERT INTO market_data_import_runs (provider,run_type,status) VALUES ('Kraken','backfill','DONE')"))
        db.execute(text("INSERT INTO market_analysis_runs (engine_name,timeframe,status) VALUES ('wyckoff','15m','DONE')"))
        db.execute(text("INSERT INTO market_data_job_requests (job_type,status,heartbeat_at) VALUES ('analysis','running',CURRENT_TIMESTAMP)"))
        db.commit()
        statements = []
        event.listen(engine, "before_cursor_execute", lambda conn, cursor, statement, parameters, context, executemany: statements.append(statement))
        rows, total = asyncio.run(repo.admin_runs())
        assert total == 3
        by_kind = {row["kind"]: row for row in rows}
        assert set(by_kind) == {"import", "analysis", "job"}
        assert by_kind["import"]["heartbeat"]["at"] is None
        assert by_kind["analysis"]["heartbeat"]["at"] is None
        assert by_kind["job"]["heartbeat"]["at"] is not None
        # PostgreSQL resolves the initial two NULL UNION branches as text unless typed.
        for statement in statements:
            assert statement.count("CAST(NULL AS TIMESTAMP) heartbeat_at") == 2

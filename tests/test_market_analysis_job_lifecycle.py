import asyncio

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from signalmaker.market_data.repository import MarketDataRepository


def test_atomic_claim_lifecycle_and_heartbeat():
    engine = create_engine("sqlite://")
    with Session(engine) as db:
        repo = MarketDataRepository(db)
        job_id = asyncio.run(repo.create_job_request("analysis", payload={"engine": "momentum"}))
        db.commit()
        claimed = asyncio.run(repo.claim_next_analysis_job("worker-a"))
        db.commit()
        assert claimed["id"] == job_id
        assert claimed["status"] == "running"
        assert claimed["attempts"] == 1
        assert asyncio.run(repo.claim_next_analysis_job("worker-b")) is None
        assert asyncio.run(repo.heartbeat_job(job_id, "worker-b")) is False
        assert asyncio.run(repo.heartbeat_job(job_id, "worker-a")) is True
        asyncio.run(repo.update_job_request(job_id, "failed", result={"last_error": "final cause"}))
        db.commit()
        row = db.execute(text("SELECT status,worker_id,started_at,heartbeat_at,finished_at,last_error FROM market_data_job_requests WHERE id=:id"), {"id": job_id}).one()
        assert row.status == "failed"
        assert row.worker_id == "worker-a"
        assert all((row.started_at, row.heartbeat_at, row.finished_at))
        assert row.last_error == "final cause"

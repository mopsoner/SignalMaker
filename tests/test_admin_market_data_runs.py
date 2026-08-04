from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.api.deps import get_db
from app.api.routes import admin_market_data
from app.core.config import settings
from signalmaker.market_data.repository import MarketDataRepository


def _client():
    engine = create_engine("sqlite://", poolclass=StaticPool,
                           connect_args={"check_same_thread": False})
    db = Session(engine)
    MarketDataRepository(db).ensure_schema()
    app = FastAPI()
    app.include_router(admin_market_data.router)
    app.dependency_overrides[get_db] = lambda: db
    return TestClient(app), db


def test_run_history_filters_paginates_and_normalizes_shape():
    client, db = _client()
    db.execute(text("""INSERT INTO market_analysis_runs
        (engine_name, timeframe, status, total_assets, success_count, failed_count, metadata)
        VALUES ('momentum', '15m', 'RUNNING', 4, 2, 1,
                '{"workflow_version":"v2","region":"EU"}')"""))
    db.execute(text("INSERT INTO market_data_import_runs (provider, run_type, status) VALUES ('IBKR','backfill','DONE')"))
    db.commit()

    response = client.get("/admin/market-data/runs?kind=analysis&status=running&engine=momentum&limit=1")
    assert response.status_code == 200
    payload = response.json()
    assert payload["pagination"] == {"limit": 1, "offset": 0, "total": 1}
    run = payload["items"][0]
    assert run["kind"] == "analysis"
    assert run["filters"]["region"] == "EU"
    assert run["counters"] == {"total": 4, "processed": 3, "succeeded": 2, "failed": 1}
    assert run["progress"]["percent"] == 75.0
    assert run["workflow_version"] == "v2"
    assert run["error"] is None


def test_cancel_and_retry_enforce_permissions_and_status_transitions():
    client, db = _client()
    db.execute(text("""INSERT INTO market_data_job_requests
        (job_type, status, attempts, worker_id, heartbeat_at, last_error)
        VALUES ('analysis', 'running', 1, 'worker-1', CURRENT_TIMESTAMP, 'boom')"""))
    job_id = db.execute(text("SELECT id FROM market_data_job_requests")).scalar_one()
    db.commit()
    cancel_url = f"/admin/market-data/runs/job/{job_id}/cancel"

    assert client.post(cancel_url).status_code == 403
    response = client.post(cancel_url, headers={"x-operator-key": settings.admin_token})
    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"

    retry_url = f"/admin/market-data/runs/job/{job_id}/retry"
    assert client.post(retry_url, headers={"x-operator-key": "wrong"}).status_code == 403
    response = client.post(retry_url, headers={"x-operator-key": settings.admin_token})
    assert response.status_code == 200
    assert response.json()["status"] == "queued"
    assert response.json()["heartbeat"] == {"at": None, "worker_id": None}

    # A queued item cannot be retried again, and non-queue run models reject retry.
    assert client.post(retry_url, headers={"x-operator-key": settings.admin_token}).status_code == 409
    assert client.post(f"/admin/market-data/runs/analysis/{job_id}/retry",
                       headers={"x-operator-key": settings.admin_token}).status_code == 409

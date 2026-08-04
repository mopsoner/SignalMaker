from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.api.deps import get_db
from app.api.routes import admin_market_data
from signalmaker.market_data.repository import MarketDataRepository


TABLES = (
    "market_analysis_results", "market_analysis_runs", "market_data_job_requests",
    "market_data_import_runs", "stock_etf_candles", "market_assets", "market_universes",
)


def _client_and_db():
    engine = create_engine("sqlite://", poolclass=StaticPool,
                           connect_args={"check_same_thread": False})
    db = Session(engine)
    MarketDataRepository(db).ensure_schema()
    app = FastAPI()
    app.include_router(admin_market_data.router)
    app.dependency_overrides[get_db] = lambda: db
    return TestClient(app), db


def _seed(db):
    db.execute(text("CREATE TABLE unrelated_records (value TEXT NOT NULL)"))
    db.execute(text("INSERT INTO unrelated_records VALUES ('keep me')"))
    db.execute(text("INSERT INTO market_universes (id, name) VALUES ('u1', 'Test')"))
    db.execute(text("INSERT INTO market_assets (id, universe_id, symbol, provider_symbol, asset_type) VALUES ('a1', 'u1', 'ABC', 'ABC', 'ETF')"))
    db.execute(text("INSERT INTO stock_etf_candles (asset_id, provider_symbol, timeframe, timestamp, open, high, low, close) VALUES ('a1', 'ABC', '1d', CURRENT_TIMESTAMP, 1, 2, 1, 2)"))
    db.execute(text("INSERT INTO market_data_import_runs (provider, run_type, status) VALUES ('IBKR', 'test', 'done')"))
    db.execute(text("INSERT INTO market_analysis_runs (id, engine_name, universe_id, status) VALUES (1, 'momentum', 'u1', 'done')"))
    db.execute(text("INSERT INTO market_analysis_results (analysis_run_id, asset_id, engine_name, timeframe) VALUES (1, 'a1', 'momentum', '1d')"))
    db.execute(text("INSERT INTO market_data_job_requests (job_type, status) VALUES ('backfill', 'queued')"))
    db.commit()


def test_clear_all_market_data_returns_counts_and_preserves_unrelated_data():
    client, db = _client_and_db()
    _seed(db)

    response = client.delete("/admin/market-data")

    assert response.status_code == 200
    assert response.json() == {"deleted": 7, "details": {table: 1 for table in TABLES}}
    assert all(db.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one() == 0 for table in TABLES)
    assert db.execute(text("SELECT value FROM unrelated_records")).scalar_one() == "keep me"


def test_clear_all_market_data_rolls_back_every_delete_on_failure():
    client, db = _client_and_db()
    _seed(db)
    db.execute(text("""
        CREATE TRIGGER reject_asset_delete BEFORE DELETE ON market_assets
        BEGIN SELECT RAISE(ABORT, 'synthetic delete failure'); END
    """))
    db.commit()

    response = client.delete("/admin/market-data")

    assert response.status_code == 500
    assert response.json()["detail"] == "Failed to clear all stock/ETF market data"
    assert all(db.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one() == 1 for table in TABLES)


def test_csv_export_rejects_unknown_kind():
    client, _ = _client_and_db()

    response = client.get("/api/v1/stocks-etfs/export.csv?kind=unsupported")

    assert response.status_code == 422

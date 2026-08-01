import sqlite3
from decimal import Decimal

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.api.deps import get_db
from app.api.routes import admin_market_data


def test_ibkr_ingest_creates_missing_asset_and_keeps_existing_asset():
    sqlite3.register_adapter(Decimal, float)
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    db = Session(engine)
    app = FastAPI()
    app.include_router(admin_market_data.router)

    def override_get_db():
        yield db

    app.dependency_overrides[get_db] = override_get_db
    client = TestClient(app)
    payload = {
        "provider_symbol": "AAPL.US",
        "symbol": "AAPL.US",
        "asset_type": "STOCK",
        "currency": "USD",
        "candles": [
            {
                "timestamp": "2026-07-31T00:00:00Z",
                "open": "210.00",
                "high": "214.00",
                "low": "209.00",
                "close": "213.50",
                "volume": "1000000",
            }
        ],
    }

    created = client.post("/api/v1/stocks-etfs/ibkr/candles", json=payload)

    assert created.status_code == 200
    assert created.json()["ok"] is True
    assert created.json()["asset_created"] is True
    assert created.json()["upserted"] == 1

    assets = client.get("/api/v1/stocks-etfs/assets?limit=50")
    assert assets.status_code == 200
    assert any(asset["provider_symbol"] == "AAPL.US" for asset in assets.json())

    existing = client.post("/api/v1/stocks-etfs/ibkr/candles", json=payload)
    assert existing.status_code == 200
    assert existing.json()["ok"] is True
    assert existing.json()["asset_created"] is False
    assert existing.json()["asset_id"] == created.json()["asset_id"]

    db.close()

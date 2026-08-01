import sqlite3
from datetime import datetime, timezone
from decimal import Decimal

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.api.deps import get_db
from app.api.routes import admin_market_data
from app.models.base import Base
from app.models.market_candle import MarketCandle
from app.services.market_data_service import MarketDataService
from signalmaker.market_data.repository import MarketDataRepository


def _client_and_db() -> tuple[TestClient, Session]:
    sqlite3.register_adapter(Decimal, float)
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    # Create the real crypto table first: STOCK/ETF schema initialization must coexist
    # with it and must not relax or populate any of its required legacy columns.
    Base.metadata.create_all(bind=engine)
    db = Session(engine)
    app = FastAPI()
    app.include_router(admin_market_data.router)

    def override_get_db():
        yield db

    app.dependency_overrides[get_db] = override_get_db
    return TestClient(app), db


def _payload(close="213.50", volume="1000000"):
    return {
        "provider_symbol": "AAPL.US",
        "symbol": "AAPL.US",
        "asset_type": "STOCK",
        "currency": "USD",
        "candles": [{
            "timestamp": "2026-07-31T00:00:00Z", "open": "210.00",
            "high": "214.00", "low": "209.00", "close": close, "volume": volume,
        }],
    }


def test_ibkr_ingest_is_idempotent_and_isolated_from_crypto_candles():
    client, db = _client_and_db()

    created = client.post("/api/v1/stocks-etfs/ibkr/candles", json=_payload())
    assert created.status_code == 200
    assert created.json()["ok"] is True
    assert created.json()["asset_created"] is True
    assert created.json()["upserted"] == 1

    asset_id = created.json()["asset_id"]
    assert db.execute(text("SELECT COUNT(*) FROM market_universes")).scalar_one() == 1
    assert db.execute(text("SELECT COUNT(*) FROM market_assets WHERE id=:id"), {"id": asset_id}).scalar_one() == 1
    candle = db.execute(text("SELECT * FROM stock_etf_candles WHERE asset_id=:id"), {"id": asset_id}).mappings().one()
    assert Decimal(str(candle["close"])) == Decimal("213.5")
    assert db.scalar(select(func.count()).select_from(MarketCandle)) == 0

    existing = client.post("/api/v1/stocks-etfs/ibkr/candles", json=_payload())
    assert existing.status_code == 200
    assert existing.json()["asset_created"] is False
    assert existing.json()["asset_id"] == asset_id
    assert db.execute(text("SELECT COUNT(*) FROM stock_etf_candles")).scalar_one() == 1

    updated = client.post(
        "/api/v1/stocks-etfs/ibkr/candles", json=_payload(close="215.25", volume="1200000")
    )
    assert updated.status_code == 200
    changed = db.execute(text("SELECT close, volume FROM stock_etf_candles")).mappings().one()
    assert Decimal(str(changed["close"])) == Decimal("215.25")
    assert Decimal(str(changed["volume"])) == Decimal("1200000")
    assert db.scalar(select(func.count()).select_from(MarketCandle)) == 0
    db.close()


def test_crypto_pipeline_still_writes_after_stock_etf_schema_initialization():
    _, db = _client_and_db()
    MarketDataRepository(db).ensure_schema()
    crypto = MarketCandle(
        candle_id="KRAKEN:BTCUSD:1m:1", symbol="BTCUSD", interval="1m",
        open_time=1_000, close_time=59_999, open=100.0, high=102.0,
        low=99.0, close=101.0, volume=12.0, quote_volume=1212.0,
        number_of_trades=10, taker_buy_base_volume=6.0,
        taker_buy_quote_volume=606.0, provider="KRAKEN",
        ingested_at=datetime.now(timezone.utc),
    )
    db.add(crypto)
    db.commit()

    loaded = MarketDataService(db).list_candles(symbol="BTCUSD", interval="1m")
    assert [c.candle_id for c in loaded] == [crypto.candle_id]
    assert db.execute(text("SELECT COUNT(*) FROM stock_etf_candles")).scalar_one() == 0
    db.close()

import sqlite3
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, func, select, text
from sqlalchemy.exc import IntegrityError
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


def test_crypto_and_stock_etf_tables_keep_independent_constraints_and_data():
    _, db = _client_and_db()
    repository = MarketDataRepository(db)
    repository.ensure_schema()
    # A second initialization exercises CREATE TABLE IF NOT EXISTS against both
    # the pre-existing historical table and the new STOCK/ETF table.
    repository.ensure_schema()

    crypto_columns = {
        row["name"]: row
        for row in db.execute(text("PRAGMA table_info(market_candles)")).mappings()
    }
    stock_columns = {
        row["name"]: row
        for row in db.execute(text("PRAGMA table_info(stock_etf_candles)")).mappings()
    }
    assert crypto_columns["candle_id"]["pk"] == 1
    assert crypto_columns["quote_volume"]["notnull"] == 1
    assert crypto_columns["number_of_trades"]["notnull"] == 1
    assert stock_columns["id"]["pk"] == 1
    assert stock_columns["asset_id"]["type"] == "TEXT"
    assert stock_columns["open"]["notnull"] == 1
    assert stock_columns["adjusted_close"]["notnull"] == 0
    assert stock_columns["volume"]["notnull"] == 0

    crypto = MarketCandle(
        candle_id="KRAKEN:ETHUSD:1m:1", symbol="ETHUSD", interval="1m",
        open_time=1_000, close_time=59_999, open=10.0, high=12.0,
        low=9.0, close=11.0, volume=2.0, quote_volume=22.0,
        number_of_trades=3, taker_buy_base_volume=1.0,
        taker_buy_quote_volume=11.0, provider="KRAKEN",
        ingested_at=datetime.now(timezone.utc),
    )
    db.add(crypto)
    asset_id = db.execute(text("""
        INSERT INTO market_assets (symbol, provider_symbol, asset_type)
        VALUES ('AAPL', 'AAPL.US', 'STOCK') RETURNING id
    """)).scalar_one()
    db.execute(text("""
        INSERT INTO stock_etf_candles
          (asset_id, provider_symbol, timeframe, timestamp, open, high, low, close)
        VALUES (:asset_id, 'AAPL.US', '1d', '2026-07-31 00:00:00', 210, 214, 209, 213.5)
    """), {"asset_id": asset_id})
    db.commit()

    with pytest.raises(IntegrityError):
        db.execute(text("""
            INSERT INTO stock_etf_candles
              (asset_id, provider_symbol, timeframe, timestamp, high, low, close)
            VALUES (:asset_id, 'AAPL.US', '1d', '2026-08-01 00:00:00', 214, 209, 213.5)
        """), {"asset_id": asset_id})
    db.rollback()

    assert db.scalar(select(func.count()).select_from(MarketCandle)) == 1
    assert db.get(MarketCandle, crypto.candle_id).provider == "KRAKEN"
    assert db.execute(text("SELECT COUNT(*) FROM stock_etf_candles")).scalar_one() == 1
    assert db.execute(text("SELECT provider FROM stock_etf_candles")).scalar_one() == "IBKR"
    db.close()


@pytest.mark.parametrize("starting_schema", ["empty", "crypto_only", "partial_stock"])
def test_stock_etf_schema_upgrade_is_idempotent_and_never_alters_crypto_table(starting_schema):
    engine = create_engine("sqlite://")
    if starting_schema == "crypto_only":
        Base.metadata.create_all(bind=engine)
    elif starting_schema == "partial_stock":
        with engine.begin() as connection:
            connection.execute(text(
                "CREATE TABLE stock_etf_candles (id INTEGER PRIMARY KEY, asset_id TEXT, open NUMERIC)"
            ))
            # A matching name with the wrong definition must not fool introspection.
            connection.execute(text(
                "CREATE INDEX uq_stock_etf_candles_asset_provider_time "
                "ON stock_etf_candles (asset_id)"
            ))

    statements = []

    def record_statement(_conn, _cursor, statement, _parameters, _context, _executemany):
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", record_statement)
    with Session(engine) as db:
        repository = MarketDataRepository(db)
        repository.ensure_schema()
        repository.ensure_schema()

        columns = {
            row["name"]
            for row in db.execute(text("PRAGMA table_info(stock_etf_candles)")).mappings()
        }
        assert {
            "asset_id", "provider", "provider_symbol", "timeframe", "timestamp",
            "open", "high", "low", "close", "adjusted_close", "volume",
            "created_at", "updated_at",
        } <= columns

        indexes = {
            row["name"]: row
            for row in db.execute(text("PRAGMA index_list(stock_etf_candles)")).mappings()
        }
        assert indexes["uq_stock_etf_candles_asset_provider_time"]["unique"] == 1
        assert indexes["uq_stock_etf_candles_asset_provider_time"]["partial"] == 0
        unique_columns = [
            row["name"]
            for row in db.execute(text(
                "PRAGMA index_info(uq_stock_etf_candles_asset_provider_time)"
            )).mappings()
        ]
        assert unique_columns == ["asset_id", "provider", "timeframe", "timestamp"]
        assert "idx_stock_etf_candles_asset_timeframe_timestamp" in indexes
        assert "idx_stock_etf_candles_symbol_timeframe_timestamp" in indexes
        assert "uq_market_candles_asset_provider_time" not in indexes

    ddl = "\n".join(statements).lower()
    assert "alter table market_candles" not in ddl

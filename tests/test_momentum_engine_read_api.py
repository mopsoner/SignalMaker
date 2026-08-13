from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.api.deps import get_db
from app.api.routes import momentum_engine
from app.models.base import Base
from app.models.momentum_engine import MomentumEnginePosition, MomentumEngineTrade


def _client_and_db() -> tuple[TestClient, Session]:
    engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    db = Session(engine)
    app = FastAPI()
    app.include_router(momentum_engine.router, prefix="/api/v1/momentum-engine")
    app.dependency_overrides[get_db] = lambda: db
    return TestClient(app), db


def _position(position_id: str, *, scope: str = "crypto", strategy: str = "momentum_rotation_v1", status: str = "open", age: int = 0):
    return MomentumEnginePosition(
        position_id=position_id, market_scope=scope, strategy=strategy, symbol=position_id.upper(),
        status=status, quantity=2, entry_price=10, entry_value=20,
        opened_at=datetime.now(timezone.utc) - timedelta(hours=age),
    )


def _trade(trade_id: str, *, scope: str = "crypto", strategy: str = "momentum_rotation_v1", age: int = 0):
    return MomentumEngineTrade(
        trade_id=trade_id, market_scope=scope, strategy=strategy, action="BUY", symbol="BTC/USD",
        price=10, quantity=2, value=20, pnl=0, reason="ranked first",
        created_at=datetime.now(timezone.utc) - timedelta(hours=age),
    )


def test_positions_are_scoped_filtered_sorted_and_paginated():
    client, db = _client_and_db()
    db.add_all([
        _position("new"), _position("old", status="closed", age=2),
        _position("stock", scope="stock_etf"), _position("wyckoff", strategy="wyckoff_v1"),
    ])
    db.commit()

    response = client.get("/api/v1/momentum-engine/positions?status=open&limit=1&offset=0")

    assert response.status_code == 200
    assert response.json()["total"] == 1
    assert [row["position_id"] for row in response.json()["items"]] == ["new"]
    all_rows = client.get("/api/v1/momentum-engine/positions?limit=10").json()["items"]
    assert [row["position_id"] for row in all_rows] == ["new", "old"]


def test_trades_never_expose_other_scopes_or_strategies_and_are_newest_first():
    client, db = _client_and_db()
    db.add_all([
        _trade("new"), _trade("old", age=2),
        _trade("stock", scope="stock_etf"), _trade("wyckoff", strategy="wyckoff_v1"),
    ])
    db.commit()

    payload = client.get("/api/v1/momentum-engine/trades?limit=10").json()

    assert payload["total"] == 2
    assert [row["trade_id"] for row in payload["items"]] == ["new", "old"]

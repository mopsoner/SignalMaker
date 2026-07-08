from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import sessionmaker

from app.api import deps
from app.main import app
from app.core.config import settings
from app.models.base import Base
from app.models.trade_candidate import TradeCandidate


REQUIRED_DECISION_FIELDS = {
    "decision_action",
    "symbol",
    "target_symbol",
    "status",
    "reason",
    "order_ids",
    "fill_ids",
}


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(settings, "create_tables_on_boot", False)
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    Base.metadata.create_all(engine)
    TestingSessionLocal = sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        future=True,
    )

    def override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[deps.get_db] = override_get_db
    try:
        with TestClient(app) as test_client:
            yield test_client, TestingSessionLocal
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(engine)
        engine.dispose()


def seed_open_momentum_candidate(session_factory) -> None:
    db = session_factory()
    try:
        db.add(
            TradeCandidate(
                candidate_id="momentum-ALLUSDC-open",
                symbol="ALLUSDC",
                side="long",
                stage="momentum",
                status="open",
                score=42.0,
                entry_price=1.0,
                stop_price=None,
                target_price=2.0,
                rr_ratio=2.0,
                execution_target={"source": "momentum_ranking"},
                payload={"source": "momentum_candidates"},
            )
        )
        db.commit()
    finally:
        db.close()


def assert_decision_contract(payload: dict) -> None:
    assert REQUIRED_DECISION_FIELDS.issubset(payload.keys())
    assert isinstance(payload["order_ids"], list)
    assert isinstance(payload["fill_ids"], list)


def test_public_momentum_decision_returns_open_candidate(client):
    test_client, session_factory = client
    seed_open_momentum_candidate(session_factory)

    response = test_client.get("/momentum-engine/decision")

    assert response.status_code == 200
    payload = response.json()
    assert_decision_contract(payload)
    assert payload["decision_action"] == "BUY"
    assert payload["symbol"] == "ALLUSDC"
    assert payload["target_symbol"] == "ALLUSDC"
    assert payload["status"] == "ready"


def test_versioned_momentum_decision_route_remains_available(client):
    test_client, session_factory = client
    seed_open_momentum_candidate(session_factory)

    response = test_client.get("/api/v1/momentum-engine/decision")

    assert response.status_code == 200
    payload = response.json()
    assert_decision_contract(payload)
    assert payload["symbol"] == "ALLUSDC"


def test_public_momentum_run_once_returns_decision_and_execution_result(client):
    test_client, session_factory = client
    seed_open_momentum_candidate(session_factory)

    response = test_client.post("/executor/momentum/run-once")

    assert response.status_code == 200
    payload = response.json()
    assert set(payload) == {"decision", "result"}
    assert_decision_contract(payload["decision"])
    assert_decision_contract(payload["result"])
    assert payload["decision"]["status"] == "executed"
    assert payload["result"]["status"] == "executed"
    assert payload["decision"]["order_ids"]
    assert payload["decision"]["fill_ids"]


def test_momentum_decision_returns_idle_contract_without_candidate(client):
    test_client, _ = client

    response = test_client.get("/momentum-engine/decision")

    assert response.status_code == 200
    payload = response.json()
    assert_decision_contract(payload)
    assert payload["decision_action"] == "HOLD"
    assert payload["status"] == "idle"
    assert payload["reason"] == "no_open_momentum_candidate"

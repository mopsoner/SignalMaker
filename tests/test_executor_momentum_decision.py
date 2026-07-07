from __future__ import annotations

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.api import deps
from app.main import app
from app.models.base import Base
from app.models.trade_candidate import TradeCandidate

DISPLAY_FIELDS = {
    "decision_action",
    "symbol",
    "target_symbol",
    "status",
    "reason",
    "order_ids",
    "fill_ids",
}


def _client_with_db():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    def override_get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[deps.get_db] = override_get_db
    return TestClient(app), SessionLocal, engine


def teardown_function():
    app.dependency_overrides.clear()


def _seed_candidate(SessionLocal):
    db = SessionLocal()
    try:
        db.add(
            TradeCandidate(
                candidate_id="momentum-BTCUSDC-open",
                symbol="BTCUSDC",
                side="long",
                stage="momentum",
                status="open",
                score=42.0,
                entry_price=100.0,
                stop_price=90.0,
                target_price=120.0,
                rr_ratio=2.0,
                payload={"source": "momentum_decision"},
            )
        )
        db.commit()
    finally:
        db.close()


def test_get_momentum_engine_decision_exposes_display_contract():
    client, SessionLocal, engine = _client_with_db()
    try:
        _seed_candidate(SessionLocal)

        response = client.get("/momentum-engine/decision")

        assert response.status_code == 200
        payload = response.json()
        assert DISPLAY_FIELDS <= payload.keys()
        assert payload["decision_action"] == "BUY"
        assert payload["symbol"] == "BTCUSDC"
        assert payload["target_symbol"] == "BTCUSDC"
        assert payload["status"] == "open"
        assert payload["reason"] == "open_momentum_candidate_ready"
        assert payload["order_ids"] == []
        assert payload["fill_ids"] == []
        assert client.get("/api/v1/momentum-engine/decision").status_code == 200
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


def test_post_executor_momentum_run_once_executes_and_returns_display_contract():
    client, SessionLocal, engine = _client_with_db()
    try:
        _seed_candidate(SessionLocal)

        response = client.post("/executor/momentum/run-once?quantity=0.25&mode=paper")

        assert response.status_code == 200
        payload = response.json()
        decision = payload["decision"]
        assert DISPLAY_FIELDS <= decision.keys()
        assert decision["decision_action"] == "BUY"
        assert decision["symbol"] == "BTCUSDC"
        assert decision["target_symbol"] == "BTCUSDC"
        assert decision["status"] == "executed"
        assert decision["reason"] == "momentum_execution_completed"
        assert decision["order_ids"]
        assert decision["fill_ids"]
        assert payload["execution"]["executed"][0]["candidate_id"] == "momentum-BTCUSDC-open"
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()

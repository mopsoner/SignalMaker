from __future__ import annotations

import requests
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models.base import Base
from app.models.app_setting import AppSetting
from app.models.trade_candidate import TradeCandidate
from app.services.executor_service import ExecutorService
from app.services.momentum_candidate_sync_service import MomentumCandidateSyncService
from app.services.trade_candidate_service import TradeCandidateService


@pytest.fixture()
def db_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(engine)
        engine.dispose()


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.url = "https://central.test/api/v1/momentum-candidates"

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def remote_candidate(**overrides):
    payload = {
        "candidate_id": "remote-btc",
        "symbol": "BTCUSDT",
        "side": "long",
        "stage": "momentum",
        "status": "momentum_ready",
        "score": 8.5,
        "entry_price": 100.0,
        "stop_price": 90.0,
        "target_price": 120.0,
        "rr_ratio": 2.0,
        "execution_target": {"notional": 25},
        "liquidity_context": {"spread": "ok"},
        "notes": "ready",
        "payload": {"rank": 1},
    }
    payload.update(overrides)
    return payload


def patch_get(monkeypatch, payload=None, exc=None):
    calls = []

    def fake_get(url, **kwargs):
        calls.append({"url": url, **kwargs})
        if exc:
            raise exc
        return FakeResponse(payload if payload is not None else [remote_candidate()])

    monkeypatch.setattr("app.services.momentum_candidate_sync_service.requests.get", fake_get)
    return calls


def test_sync_uses_existing_signalmaker_base_url_setting(db_session, monkeypatch):
    db_session.add(AppSetting(category="momentum", key="signalmaker_base_url", value="https://central.local"))
    db_session.commit()
    calls = patch_get(monkeypatch)

    MomentumCandidateSyncService(db_session).sync()

    assert calls[0]["url"] == "https://central.local/api/v1/momentum-candidates"


def test_sync_momentum_candidates_success(db_session, monkeypatch):
    calls = patch_get(monkeypatch)

    summary = MomentumCandidateSyncService(db_session).sync(limit=25, min_momentum_score=3, min_rr=1.2, require_wyckoff_context=False)

    assert summary["fetched"] == 1
    assert summary["upserted"] == 1
    assert summary["skipped"] == []
    assert summary["errors"] == []
    assert calls[0]["params"] == {"limit": 25, "min_momentum_score": 3, "min_rr": 1.2, "require_wyckoff_context": False}
    row = db_session.get(TradeCandidate, "momentum-BTCUSDT-open")
    assert row is not None
    assert row.symbol == "BTCUSDT"
    assert row.status == "open"
    assert row.payload["source"] == "momentum_candidates"
    assert row.payload["remote_candidate_id"] == "remote-btc"


def test_skip_candidate_without_entry_stop_target(db_session, monkeypatch):
    patch_get(monkeypatch, [remote_candidate(entry_price=None)])

    summary = MomentumCandidateSyncService(db_session).sync()

    assert summary["fetched"] == 1
    assert summary["upserted"] == 0
    assert summary["skipped"][0]["reason"] == "missing_entry_stop_or_target"
    assert db_session.get(TradeCandidate, "momentum-BTCUSDT-open") is None


def test_remote_status_momentum_ready_becomes_local_open(db_session, monkeypatch):
    patch_get(monkeypatch, [remote_candidate(status="momentum_ready")])

    MomentumCandidateSyncService(db_session).sync()

    assert db_session.get(TradeCandidate, "momentum-BTCUSDT-open").status == "open"


def test_does_not_overwrite_executed_local_candidate(db_session, monkeypatch):
    existing = TradeCandidate(
        candidate_id="momentum-BTCUSDT-open",
        symbol="BTCUSDT",
        side="long",
        stage="momentum",
        status="executed",
        score=1.0,
        entry_price=50.0,
        stop_price=45.0,
        target_price=60.0,
        payload={"source": "old"},
    )
    db_session.add(existing)
    db_session.commit()
    patch_get(monkeypatch, [remote_candidate(entry_price=100.0)])

    summary = MomentumCandidateSyncService(db_session).sync()

    row = db_session.get(TradeCandidate, "momentum-BTCUSDT-open")
    assert summary["upserted"] == 0
    assert summary["skipped"][0]["reason"] == "local_candidate_already_executed"
    assert row.status == "executed"
    assert row.entry_price == 50.0
    assert row.payload == {"source": "old"}


def test_executor_can_execute_synced_candidate_in_paper_mode(db_session, monkeypatch):
    patch_get(monkeypatch, [remote_candidate()])

    result = ExecutorService(db_session).execute_open_candidates(limit=10, quantity=0.25, mode="paper", sync_momentum_first=True)

    assert result["sync"]["upserted"] == 1
    assert result["executed"][0]["candidate_id"] == "momentum-BTCUSDT-open"
    assert db_session.get(TradeCandidate, "momentum-BTCUSDT-open").status == "executed"


def test_api_failure_returns_clean_error_and_does_not_crash_executor(db_session, monkeypatch):
    TradeCandidateService(db_session).upsert_open_candidate(
        candidate_id="ETHUSDT-accumulation",
        symbol="ETHUSDT",
        side="long",
        stage="accumulation",
        score=4.0,
        entry_price=100.0,
        stop_price=90.0,
        target_price=120.0,
        rr_ratio=2.0,
        execution_target=None,
        liquidity_context=None,
        notes=None,
        payload={"source": "wyckoff"},
    )
    patch_get(monkeypatch, exc=requests.ConnectionError("central down"))

    result = ExecutorService(db_session).execute_open_candidates(limit=10, quantity=1.0, mode="paper", sync_momentum_first=True)

    assert result["sync"]["errors"][0]["reason"] == "api_error"
    assert result["executed"][0]["candidate_id"] == "ETHUSDT-accumulation"

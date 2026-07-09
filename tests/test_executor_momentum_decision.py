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
from app.models.position import Position


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


def seed_open_position(session_factory, *, symbol: str = "ALLUSDC") -> str:
    from app.services.position_service import PositionService

    db = session_factory()
    try:
        position = PositionService(db).create_position(
            symbol=symbol,
            side="long",
            quantity=2.0,
            entry_price=10.0,
            mark_price=12.0,
            stop_price=None,
            target_price=15.0,
            meta={"candidate_id": f"momentum-{symbol}-open", "mode": "paper"},
        )
        return position.position_id
    finally:
        db.close()


def seed_named_momentum_candidate(session_factory, *, symbol: str) -> None:
    db = session_factory()
    try:
        db.add(
            TradeCandidate(
                candidate_id=f"momentum-{symbol}-open",
                symbol=symbol,
                side="long",
                stage="momentum",
                status="open",
                score=50.0,
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


def execute_with_stubbed_decision(session_factory, monkeypatch, decision: dict) -> dict:
    from app.services.executor_service import ExecutorService
    from app.services.momentum_decision_service import MomentumDecisionService

    monkeypatch.setattr(MomentumDecisionService, "decision", lambda self: decision)
    db = session_factory()
    try:
        return ExecutorService(db).execute_momentum_decision(mode="paper")
    finally:
        db.close()


def test_momentum_executor_sell_closes_matching_open_position(client, monkeypatch):
    _, session_factory = client
    position_id = seed_open_position(session_factory, symbol="ALLUSDC")

    result = execute_with_stubbed_decision(
        session_factory,
        monkeypatch,
        {
            "decision_action": "sell",
            "symbol": "ALLUSDC",
            "target_symbol": None,
            "status": "ready",
            "reason": "test_sell",
            "order_ids": [],
            "fill_ids": [],
        },
    )

    assert_decision_contract(result)
    assert result["decision_action"] == "SELL"
    assert result["symbol"] == "ALLUSDC"
    assert result["status"] == "executed"
    assert result["reason"] == "position_closed"
    assert result["order_ids"]
    assert result["fill_ids"]

    db = session_factory()
    try:
        assert db.get(Position, position_id).status == "closed"
    finally:
        db.close()


def test_momentum_executor_rotate_closes_source_before_buying_target(client, monkeypatch):
    _, session_factory = client
    source_position_id = seed_open_position(session_factory, symbol="ALLUSDC")
    seed_named_momentum_candidate(session_factory, symbol="BTCUSDC")

    result = execute_with_stubbed_decision(
        session_factory,
        monkeypatch,
        {
            "decision_action": "rotate",
            "symbol": "ALLUSDC",
            "target_symbol": "BTCUSDC",
            "status": "ready",
            "reason": "test_rotate",
            "order_ids": [],
            "fill_ids": [],
        },
    )

    assert_decision_contract(result)
    assert result["decision_action"] == "ROTATE"
    assert result["symbol"] == "ALLUSDC"
    assert result["target_symbol"] == "BTCUSDC"
    assert result["status"] == "executed"
    assert result["reason"] == "momentum_rotated"
    assert len(result["order_ids"]) >= 2
    assert len(result["fill_ids"]) >= 2

    db = session_factory()
    try:
        assert db.get(Position, source_position_id).status == "closed"
        assert any(row.symbol == "BTCUSDC" and row.status == "open" for row in db.query(Position).all())
    finally:
        db.close()


@pytest.mark.parametrize("action", ["WAIT", "HOLD"])
def test_momentum_executor_wait_and_hold_are_explicit_noops(client, monkeypatch, action):
    _, session_factory = client

    result = execute_with_stubbed_decision(
        session_factory,
        monkeypatch,
        {
            "decision_action": action.lower(),
            "symbol": "ALLUSDC",
            "target_symbol": "BTCUSDC",
            "status": "ready",
            "reason": f"test_{action.lower()}",
            "order_ids": [],
            "fill_ids": [],
        },
    )

    assert_decision_contract(result)
    assert result["decision_action"] == action
    assert result["symbol"] == "ALLUSDC"
    assert result["target_symbol"] == "BTCUSDC"
    assert result["status"] == "skipped"
    assert result["reason"] == f"test_{action.lower()}"
    assert result["order_ids"] == []
    assert result["fill_ids"] == []


def test_momentum_decision_schema_defines_expected_actions_and_statuses():
    from app.schemas.momentum import (
        ALLOWED_MOMENTUM_DECISION_ACTIONS,
        EXPECTED_MOMENTUM_DECISION_STATUSES,
        MomentumDecision,
    )

    assert ALLOWED_MOMENTUM_DECISION_ACTIONS == {"BUY", "SELL", "ROTATE", "WAIT", "HOLD"}
    assert EXPECTED_MOMENTUM_DECISION_STATUSES == {"ready", "idle", "waiting", "skipped", "executed", "error"}

    for action in ALLOWED_MOMENTUM_DECISION_ACTIONS:
        payload = MomentumDecision(
            decision_action=action,
            symbol="ALLUSDC",
            target_symbol="BTCUSDC" if action == "ROTATE" else "ALLUSDC",
            status="ready",
            reason=f"test_{action.lower()}",
        ).model_dump()
        for field in ("decision_action", "symbol", "target_symbol", "status", "reason"):
            assert field in payload


def test_momentum_executor_rejects_unsupported_action_before_execution(client, monkeypatch):
    _, session_factory = client

    result = execute_with_stubbed_decision(
        session_factory,
        monkeypatch,
        {
            "decision_action": "CANCEL",
            "symbol": "ALLUSDC",
            "target_symbol": "ALLUSDC",
            "status": "ready",
            "reason": "test_unsupported",
            "order_ids": [],
            "fill_ids": [],
        },
    )

    assert_decision_contract(result)
    assert result["decision_action"] == "CANCEL"
    assert result["status"] == "skipped"
    assert result["reason"] == "unsupported_momentum_decision_action:CANCEL"
    assert result["order_ids"] == []
    assert result["fill_ids"] == []

class FakeLiveAdapter:
    exchange_name = "kraken"

    def __init__(self):
        self.client = type("Client", (), {"dry_run": True})()

    def is_configured(self):
        return True

    def current_price(self, symbol):
        return 1.0

    def normalize_order(self, symbol, quantity, target_price, stop_price=None):
        return {"symbol": symbol, "quantity": quantity, "mark_price": 1.0, "target_price": target_price, "stop_price": stop_price}


class FakeMomentumMarginManager:
    def __init__(self, *, fail_leverages=()):
        self.fail_leverages = set(fail_leverages)
        self.open_calls = []
        self.sell_calls = []

    def open_long_with_margin_take_profit(self, *, symbol, quote_amount, target_price, leverage=None):
        self.open_calls.append({"symbol": symbol, "quote_amount": quote_amount, "target_price": target_price, "leverage": leverage})
        if leverage in self.fail_leverages:
            raise RuntimeError(f"leverage rejected:{leverage}")
        return {
            "symbol": symbol,
            "quantity": "2.0",
            "entry_price": "1.25",
            "entry_order_id": f"entry-{symbol}-{leverage}",
            "tp_order_id": f"tp-{symbol}-{leverage}",
            "entry_payload": {"orderId": f"entry-{symbol}-{leverage}", "status": "FILLED"},
            "tp_payload": {"orderId": f"tp-{symbol}-{leverage}", "status": "OPEN"},
        }

    def sell_all_margin_base(self, *, symbol, quantity=None):
        self.sell_calls.append({"symbol": symbol, "quantity": quantity})
        return {"status": "FILLED", "symbol": symbol, "quantity": "2.0", "price": 12.5, "order_id": f"sell-{symbol}"}


def execute_live_with_stubbed_decision(session_factory, monkeypatch, decision: dict, manager: FakeMomentumMarginManager) -> dict:
    from app.services import executor_service as executor_module
    from app.services.executor_service import ExecutorService
    from app.services.momentum_decision_service import MomentumDecisionService

    monkeypatch.setattr(MomentumDecisionService, "decision", lambda self: decision)
    monkeypatch.setattr(executor_module, "margin_leverage_attempts", lambda: (5, 3, 2))
    monkeypatch.setattr(ExecutorService, "_kraken_margin_order_manager", lambda self: manager)
    db = session_factory()
    try:
        service = ExecutorService(db)
        service.kraken = FakeLiveAdapter()
        return service.execute_momentum_decision(mode="live")
    finally:
        db.close()


def test_momentum_executor_buy_uses_leveraged_fallback_attempts(client, monkeypatch):
    _, session_factory = client
    seed_named_momentum_candidate(session_factory, symbol="BTCUSDC")
    manager = FakeMomentumMarginManager(fail_leverages={5})

    result = execute_live_with_stubbed_decision(
        session_factory,
        monkeypatch,
        {"decision_action": "BUY", "symbol": "BTCUSDC", "target_symbol": "BTCUSDC", "status": "ready", "reason": "test_buy", "order_ids": [], "fill_ids": []},
        manager,
    )

    assert_decision_contract(result)
    assert result["status"] == "executed"
    assert [call["leverage"] for call in manager.open_calls] == [5, 3]
    assert result["result"]["mode"] == "margin"
    assert result["result"]["leverage"] == 3
    assert result["order_ids"]
    assert result["fill_ids"]


def test_momentum_executor_sell_uses_leveraged_close_for_margin_position(client, monkeypatch):
    _, session_factory = client
    position_id = seed_open_position(session_factory, symbol="ETHUSDC")
    db = session_factory()
    try:
        position = db.get(Position, position_id)
        position.meta = {**(position.meta or {}), "mode": "margin"}
        db.commit()
    finally:
        db.close()
    manager = FakeMomentumMarginManager()

    result = execute_live_with_stubbed_decision(
        session_factory,
        monkeypatch,
        {"decision_action": "SELL", "symbol": "ETHUSDC", "target_symbol": None, "status": "ready", "reason": "test_sell", "order_ids": [], "fill_ids": []},
        manager,
    )

    assert result["status"] == "executed"
    assert manager.sell_calls == [{"symbol": "ETHUSDC", "quantity": 2.0}]
    db = session_factory()
    try:
        assert db.get(Position, position_id).status == "closed"
    finally:
        db.close()


def test_momentum_executor_rotate_uses_margin_sell_then_leveraged_buy(client, monkeypatch):
    _, session_factory = client
    source_position_id = seed_open_position(session_factory, symbol="SOLUSDC")
    db = session_factory()
    try:
        db.get(Position, source_position_id).meta = {"mode": "margin", "candidate_id": "momentum-SOLUSDC-open"}
        db.commit()
    finally:
        db.close()
    seed_named_momentum_candidate(session_factory, symbol="ADAUSDC")
    manager = FakeMomentumMarginManager(fail_leverages={5})

    result = execute_live_with_stubbed_decision(
        session_factory,
        monkeypatch,
        {"decision_action": "ROTATE", "symbol": "SOLUSDC", "target_symbol": "ADAUSDC", "status": "ready", "reason": "test_rotate", "order_ids": [], "fill_ids": []},
        manager,
    )

    assert result["status"] == "executed"
    assert manager.sell_calls == [{"symbol": "SOLUSDC", "quantity": 2.0}]
    assert [call["leverage"] for call in manager.open_calls] == [5, 3]
    assert result["reason"] == "momentum_rotated"


def test_momentum_executor_hold_confirms_target_position_before_ready_gate(client, monkeypatch):
    _, session_factory = client
    position_id = seed_open_position(session_factory, symbol="BTCUSDC")

    result = execute_with_stubbed_decision(
        session_factory,
        monkeypatch,
        {
            "decision_action": "hold",
            "symbol": "ALLUSDC",
            "target_symbol": "BTCUSDC",
            "status": "waiting",
            "reason": "not_ready_but_target_is_held",
            "order_ids": [],
            "fill_ids": [],
        },
    )

    assert_decision_contract(result)
    assert result["decision_action"] == "HOLD"
    assert result["status"] == "skipped"
    assert result["reason"] == "already_held"
    assert result["symbol"] == "BTCUSDC"
    assert result["target_symbol"] == "BTCUSDC"
    assert result["position_id"] == position_id


def test_momentum_executor_buy_still_requires_ready_status(client, monkeypatch):
    _, session_factory = client
    seed_named_momentum_candidate(session_factory, symbol="BTCUSDC")

    result = execute_with_stubbed_decision(
        session_factory,
        monkeypatch,
        {
            "decision_action": "buy",
            "symbol": "BTCUSDC",
            "target_symbol": "BTCUSDC",
            "status": "waiting",
            "reason": "signal_not_ready",
            "order_ids": [],
            "fill_ids": [],
        },
    )

    assert_decision_contract(result)
    assert result["decision_action"] == "BUY"
    assert result["status"] == "skipped"
    assert result["reason"] == "signal_not_ready"
    assert result["order_ids"] == []
    assert result["fill_ids"] == []

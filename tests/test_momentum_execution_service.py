from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.services.execution import momentum_execution_service as module
from app.services.execution.momentum_execution_service import MomentumExecutionService
from app.models.base import Base


class FakeExecution:
    def __init__(self):
        self.calls = []

    def buy_market(self, symbol, **kwargs):
        self.calls.append(("buy", symbol))
        return {"status": "simulated"}

    def sell_market(self, symbol, **kwargs):
        self.calls.append(("sell", symbol))
        return {"status": "simulated"}


@pytest.mark.parametrize("action", ["WAIT", "HOLD"])
def test_non_transactional_decisions_do_not_construct_exchange(action):
    service = MomentumExecutionService(SimpleNamespace())
    assert service.execute_decision({"action": action})["status"] == "skipped"
    assert service._execution_service is None


@pytest.mark.parametrize(
    ("decision", "calls"),
    [
        ({"action": "BUY", "buy_symbol": "BTCUSD"}, [("buy", "BTCUSD")]),
        ({"action": "SELL", "sell_symbol": "ETHUSD"}, [("sell", "ETHUSD")]),
        ({"action": "ROTATE", "sell_symbol": "ETHUSD", "buy_symbol": "BTCUSD"}, [("sell", "ETHUSD"), ("buy", "BTCUSD")]),
    ],
)
def test_transactional_decisions_call_execution_in_order(monkeypatch, decision, calls):
    monkeypatch.setattr(module, "settings", SimpleNamespace(momentum_execution_enabled=True, momentum_execution_mode="spot"))
    execution = FakeExecution()
    result = MomentumExecutionService(SimpleNamespace(), execution_service=execution).execute_decision(decision)
    assert result["status"] == "executed"
    assert execution.calls == calls


def test_not_due_decision_does_not_place_order(monkeypatch):
    monkeypatch.setattr(module, "settings", SimpleNamespace(momentum_execution_enabled=True, momentum_execution_mode="spot"))
    execution = FakeExecution()
    result = MomentumExecutionService(SimpleNamespace(), execution_service=execution).execute_decision({"action": "BUY", "symbol": "BTCUSD", "due_now": False})
    assert result["reason"] == "not_due"
    assert execution.calls == []


def test_live_decision_is_submitted_only_once(monkeypatch):
    monkeypatch.setattr(
        module,
        "settings",
        SimpleNamespace(
            momentum_execution_enabled=True,
            momentum_execution_mode="spot",
            kraken_dry_run=False,
        ),
    )
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    execution = FakeExecution()
    decision = {"decision_id": "decision-123", "action": "BUY", "buy_symbol": "BTCUSD"}

    with Session(engine) as db:
        first = MomentumExecutionService(db, execution_service=execution).execute_decision(decision)
        second = MomentumExecutionService(db, execution_service=execution).execute_decision(decision)

    assert first["status"] == "executed"
    assert second["reason"] == "decision_already_submitted"
    assert execution.calls == [("buy", "BTCUSD")]

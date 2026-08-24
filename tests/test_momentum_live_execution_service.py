from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.services.execution import momentum_live_execution_service as module
from app.services.execution.momentum_live_execution_service import MomentumLiveExecutionService
from app.models.base import Base


class FakeExecution:
    def __init__(self):
        self.calls = []

    def buy_market(self, symbol, **kwargs):
        self.calls.append(("buy", symbol, kwargs.get("total_notional")))
        return {"status": "simulated"}

    def sell_market(self, symbol, **kwargs):
        self.calls.append(("sell", symbol))
        return {"status": "simulated"}


@pytest.mark.parametrize("action", ["WAIT", "HOLD"])
def test_non_transactional_decisions_do_not_construct_exchange(action):
    service = MomentumLiveExecutionService(SimpleNamespace())
    assert service.execute_decision({"action": action})["status"] == "skipped"
    assert service._execution_service is None


@pytest.mark.parametrize(
    ("decision", "calls"),
    [
        ({"action": "BUY", "buy_symbol": "BTCUSD"}, [("buy", "BTCUSD", 150.0)]),
        ({"action": "SELL", "sell_symbol": "ETHUSD"}, [("sell", "ETHUSD")]),
        ({"action": "ROTATE", "sell_symbol": "ETHUSD", "buy_symbol": "BTCUSD"}, [("sell", "ETHUSD"), ("buy", "BTCUSD", 150.0)]),
    ],
)
def test_transactional_decisions_call_execution_in_order(monkeypatch, decision, calls):
    monkeypatch.setattr(module, "settings", SimpleNamespace(momentum_live_enabled=True, momentum_live_mode="spot", kraken_default_total_notional=150.0))
    execution = FakeExecution()
    result = MomentumLiveExecutionService(SimpleNamespace(), execution_service=execution).execute_decision(decision)
    assert result["status"] == "executed"
    assert execution.calls == calls


def test_not_due_decision_does_not_place_order(monkeypatch):
    monkeypatch.setattr(module, "settings", SimpleNamespace(momentum_live_enabled=True, momentum_live_mode="spot", kraken_default_total_notional=150.0))
    execution = FakeExecution()
    result = MomentumLiveExecutionService(SimpleNamespace(), execution_service=execution).execute_decision({"action": "BUY", "symbol": "BTCUSD", "due_now": False})
    assert result["reason"] == "not_due"
    assert execution.calls == []


def test_live_decision_is_submitted_only_once(monkeypatch):
    monkeypatch.setattr(
        module,
        "settings",
        SimpleNamespace(
            momentum_live_enabled=True,
            momentum_live_mode="spot",
            kraken_dry_run=False,
            kraken_default_total_notional=150.0,
        ),
    )
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    execution = FakeExecution()
    decision = {"decision_id": "decision-123", "action": "BUY", "buy_symbol": "BTCUSD"}

    with Session(engine) as db:
        first = MomentumLiveExecutionService(db, execution_service=execution).execute_decision(decision)
        second = MomentumLiveExecutionService(db, execution_service=execution).execute_decision(decision)

    assert first["status"] == "executed"
    assert second["reason"] == "decision_already_submitted"
    assert execution.calls == [("buy", "BTCUSD", 150.0)]

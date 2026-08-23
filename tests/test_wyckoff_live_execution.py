from types import SimpleNamespace

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.services import executor_service as module
from app.services.executor_service import ExecutorService
from app.models.base import Base
from app.models.candidate_execution import CandidateExecution
from app.models.position import Position
from app.models.trade_candidate import TradeCandidate


class FakeKrakenExecution:
    calls = []
    order = {"status": "filled", "executed_quantity": "2.5", "average_price": 101.0}
    fail_tp = False

    def __init__(self, _db):
        pass

    def buy_market(self, symbol, total_notional, *, mode):
        self.calls.append(("buy", symbol, total_notional, mode))
        return {"status": "filled", "order_id": "kraken-1"}

    def sell_market(self, symbol, quantity, *, mode, intent):
        self.calls.append(("sell", symbol, quantity, mode, intent))
        return {"status": "pending", "order_id": "kraken-1", "effective_leverage": 2}

    def get_order(self, symbol, order_id, *, mode):
        self.calls.append(("get", symbol, order_id, mode))
        return {"order_id": order_id, **self.order}

    def place_take_profit(self, symbol, side, quantity, price, *, mode, leverage=None):
        self.calls.append(("tp", symbol, side, quantity, price, mode, leverage))
        if self.fail_tp:
            raise RuntimeError("tp rejected")
        return {"status": "pending", "order_id": "kraken-tp"}


def _live_service(monkeypatch, tmp_path, side="bull"):
    FakeKrakenExecution.calls = []
    FakeKrakenExecution.order = {"status": "filled", "executed_quantity": "2.5", "average_price": 101.0}
    FakeKrakenExecution.fail_tp = False
    monkeypatch.setattr(module, "KrakenExecutionService", FakeKrakenExecution)
    monkeypatch.setattr(module, "settings", SimpleNamespace(
        wyckoff_live_enabled=True,
        wyckoff_live_mode="spot",
        kraken_execution_enabled=True,
        kraken_dry_run=False,
        kraken_api_key="key",
        kraken_secret_key="secret",
        kraken_margin_execution_enabled=False,
    ))
    monkeypatch.setattr(
        module,
        "load_runtime_settings",
        lambda _db: {"live": {"live_require_tp_sl": True, "live_min_total_notional_per_trade": 150, "live_max_notional_per_trade": 250}},
    )
    engine = create_engine(f"sqlite:///{tmp_path / (side + '.db')}")
    Base.metadata.create_all(engine)
    db = Session(engine)
    candidate = TradeCandidate(candidate_id="candidate-1", symbol="BTCUSD", side=side, stage="spring", status="open", score=1, entry_price=100, stop_price=90 if side == "bull" else 110, target_price=120 if side == "bull" else 80)
    db.add(candidate)
    db.add(CandidateExecution(execution_id="candidate-1-live", candidate_id="candidate-1", execution_mode="live", status="claimed"))
    db.commit()
    service = ExecutorService(db)
    service._hierarchical_target_plan = lambda _candidate: {"target_price": 120.0}
    return service, candidate, db


def test_live_bull_candidate_waits_for_fill_then_persists_protection(monkeypatch, tmp_path):
    service, candidate, db = _live_service(monkeypatch, tmp_path)

    result = service._execute_live_candidate(candidate, quantity=3)

    assert ("tp", "BTCUSD", "sell", 2.5, 120.0, "spot", None) in FakeKrakenExecution.calls
    assert result["executed_quantity"] == 2.5
    position = db.scalar(select(Position))
    assert (position.entry_order_id, position.take_profit_order_id) == ("kraken-1", "kraken-tp")
    state = db.scalar(select(CandidateExecution))
    assert (state.entry_order_id, state.take_profit_order_id) == ("kraken-1", "kraken-tp")
    assert ("buy", "BTCUSD", 250.0, "spot") in FakeKrakenExecution.calls


def test_low_price_candidate_uses_minimum_total_notional(monkeypatch, tmp_path):
    service, candidate, _db = _live_service(monkeypatch, tmp_path)
    candidate.entry_price = 0.01

    service._execute_live_candidate(candidate, quantity=3)

    assert ("buy", "BTCUSD", 150.0, "spot") in FakeKrakenExecution.calls


def test_partial_fill_stays_pending_without_exit_orders(monkeypatch, tmp_path):
    service, candidate, _db = _live_service(monkeypatch, tmp_path)
    FakeKrakenExecution.order = {"status": "open", "executed_quantity": "1.25", "average_price": 100.5}
    result = service._execute_live_candidate(candidate, quantity=3)
    assert result["pending"] is True
    assert not any(call[0] == "tp" for call in FakeKrakenExecution.calls)


def test_short_uses_buy_side_for_margin_protection(monkeypatch, tmp_path):
    service, candidate, _db = _live_service(monkeypatch, tmp_path, side="bear")
    module.settings.wyckoff_live_mode = "margin"
    service._hierarchical_target_plan = lambda _candidate: {"target_price": 80.0}
    service._execute_live_candidate(candidate, quantity=2)
    assert any(call[:3] == ("tp", "BTCUSD", "buy") for call in FakeKrakenExecution.calls)


def test_take_profit_failure_leaves_execution_unfinished(monkeypatch, tmp_path):
    service, candidate, db = _live_service(monkeypatch, tmp_path)
    FakeKrakenExecution.fail_tp = True
    import pytest
    with pytest.raises(RuntimeError, match="tp rejected"):
        service._execute_live_candidate(candidate, quantity=3)
    state = db.scalar(select(CandidateExecution))
    assert state.status == "claimed"
    assert state.entry_order_id == "kraken-1"


def test_paper_and_live_cycles_do_not_consume_each_others_claim(monkeypatch):
    candidate = SimpleNamespace(candidate_id="signal-1", side="bull", entry_price=100.0, stop_price=90.0)
    claims = {"paper": [candidate], "live": [candidate]}
    completed = []
    repository = SimpleNamespace(
        claim_open_candidates=lambda *, execution_mode, limit: claims.pop(execution_mode),
        get_pending_candidates=lambda *, execution_mode, limit: [],
        finish_execution=lambda candidate_id, *, execution_mode, error=None: completed.append(
            (candidate_id, execution_mode, error)
        ),
        release_claim=lambda *_args, **_kwargs: None,
    )
    service = object.__new__(ExecutorService)
    service.candidates = repository
    service._hierarchical_target_plan = lambda _candidate: {"target_price": 120.0}
    service._current_price_for_candidate = lambda _candidate, *, requested_mode: 100.0
    service._execute_paper_candidate = lambda row, quantity: {"candidate_id": row.candidate_id, "mode": "paper"}
    service._execute_live_candidate = lambda row, quantity: {"candidate_id": row.candidate_id, "mode": "live", "protection_installed": True}

    monkeypatch.setattr(module, "settings", SimpleNamespace(
        wyckoff_live_enabled=True,
        wyckoff_live_mode="spot",
        kraken_execution_enabled=True,
        kraken_dry_run=False,
        kraken_api_key="key",
        kraken_secret_key="secret",
        kraken_margin_execution_enabled=False,
    ))

    assert service.execute_open_candidates(mode="paper")["executed"][0]["mode"] == "paper"
    assert service.execute_open_candidates(mode="live")["executed"][0]["mode"] == "live"
    assert completed == [("signal-1", "paper", None), ("signal-1", "live", None)]

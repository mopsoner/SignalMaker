from types import SimpleNamespace

from app.services import executor_service as module
from app.services.executor_service import ExecutorService


class FakeKrakenExecution:
    calls = []

    def __init__(self, _db):
        pass

    def buy_market(self, symbol, quote_amount, *, mode):
        self.calls.append(("buy", symbol, quote_amount, mode))
        return {"status": "filled", "order_id": "kraken-1"}


def test_live_bull_candidate_submits_to_kraken_and_is_consumed(monkeypatch):
    FakeKrakenExecution.calls = []
    monkeypatch.setattr(module, "KrakenExecutionService", FakeKrakenExecution)
    monkeypatch.setattr(module, "settings", SimpleNamespace(wyckoff_live_mode="spot"))
    monkeypatch.setattr(
        module,
        "load_runtime_settings",
        lambda _db: {"live": {"live_require_tp_sl": True, "live_max_notional_per_trade": 250}},
    )
    service = object.__new__(ExecutorService)
    service.db = object()
    service.candidates = SimpleNamespace(mark_executed=lambda candidate_id: consumed.append(candidate_id))
    service._hierarchical_target_plan = lambda _candidate: {"target_price": 120.0}
    consumed = []
    candidate = SimpleNamespace(
        candidate_id="candidate-1",
        symbol="BTCUSD",
        side="bull",
        entry_price=100.0,
        stop_price=90.0,
    )

    result = service._execute_live_candidate(candidate, quantity=3)

    assert FakeKrakenExecution.calls == [("buy", "BTCUSD", 250.0, "spot")]
    assert consumed == ["candidate-1"]
    assert result["exchange_order"]["order_id"] == "kraken-1"

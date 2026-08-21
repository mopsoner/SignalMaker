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


def test_live_bull_candidate_submits_to_kraken_and_records_live_mode(monkeypatch):
    FakeKrakenExecution.calls = []
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
        lambda _db: {"live": {"live_require_tp_sl": True, "live_max_notional_per_trade": 250}},
    )
    service = object.__new__(ExecutorService)
    service.db = object()
    service._hierarchical_target_plan = lambda _candidate: {"target_price": 120.0}
    candidate = SimpleNamespace(
        candidate_id="candidate-1",
        symbol="BTCUSD",
        side="bull",
        entry_price=100.0,
        stop_price=90.0,
    )

    result = service._execute_live_candidate(candidate, quantity=3)

    assert FakeKrakenExecution.calls == [("buy", "BTCUSD", 250.0, "spot")]
    assert result["exchange_order"]["order_id"] == "kraken-1"
    assert result["mode"] == "live"


def test_paper_and_live_cycles_do_not_consume_each_others_claim(monkeypatch):
    candidate = SimpleNamespace(candidate_id="signal-1", side="bull", entry_price=100.0, stop_price=90.0)
    claims = {"paper": [candidate], "live": [candidate]}
    completed = []
    repository = SimpleNamespace(
        claim_open_candidates=lambda *, execution_mode, limit: claims.pop(execution_mode),
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
    service._execute_live_candidate = lambda row, quantity: {"candidate_id": row.candidate_id, "mode": "live"}

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

from __future__ import annotations

from types import SimpleNamespace

import raspberry_executor.sqlite_db as sqlite_db
from raspberry_executor.main import execute_candidate, report_final_events
from raspberry_executor.risk_guard import RiskGuard
from raspberry_executor.state import StateStore


class FakeKraken:
    def __init__(self):
        self.stop_loss_calls = []
        self.exit_limit_calls = []
        self.order_queries = []

    def current_price(self, symbol: str) -> float:
        return 100.0

    def place_market_entry(self, symbol: str, side: str, quantity: float) -> dict:
        return {"orderId": "entry-1", "status": "FILLED", "executedQty": str(quantity), "fills": [{"price": "100", "qty": str(quantity)}]}

    def place_exit_limit(self, symbol: str, side: str, quantity: float, price: float) -> dict:
        self.exit_limit_calls.append({"symbol": symbol, "side": side, "quantity": quantity, "price": price})
        return {"orderId": "tp-1", "status": "NEW", "price": str(price)}

    def place_stop_loss(self, *args, **kwargs):
        self.stop_loss_calls.append({"args": args, "kwargs": kwargs})
        raise AssertionError("stop-loss orders should not be created")

    def get_order(self, symbol: str, order_id: str) -> dict:
        self.order_queries.append({"symbol": symbol, "order_id": order_id})
        return {"orderId": order_id, "status": "FILLED", "price": "120"}


def state_store(tmp_path, monkeypatch) -> StateStore:
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    return StateStore()


def candidate_without_stop() -> dict:
    return {
        "candidate_id": "candidate-btc",
        "symbol": "BTCUSDT",
        "side": "long",
        "status": "open",
        "entry_price": 100.0,
        "target_price": 120.0,
    }


def test_risk_guard_accepts_candidate_without_stop_loss():
    guard = RiskGuard(["USDT"], max_candidate_age_seconds=3600)

    accepted, reason = guard.accept(candidate_without_stop(), already_executed=False)

    assert accepted is True
    assert reason == "accepted"


def test_raspberry_executor_creates_take_profit_only_position(tmp_path, monkeypatch):
    state = state_store(tmp_path, monkeypatch)
    guard = RiskGuard(["USDT"], max_candidate_age_seconds=3600)
    kraken = FakeKraken()
    settings = SimpleNamespace(order_quote_amount=25.0)

    execute_candidate(settings, kraken, state, guard, candidate_without_stop())

    position = state.open_positions()["candidate-btc"]
    assert kraken.exit_limit_calls == [{"symbol": "BTCUSDT", "side": "long", "quantity": 0.25, "price": 120.0}]
    assert kraken.stop_loss_calls == []
    assert position["tp_order_id"] == "tp-1"
    assert position["sl_order_id"] is None
    assert position["exit_strategy"] == "take_profit_only"


def test_raspberry_executor_final_report_checks_take_profit_only(tmp_path, monkeypatch):
    state = state_store(tmp_path, monkeypatch)
    state.add_open_position("candidate-btc", {
        "candidate_id": "candidate-btc",
        "signal_symbol": "BTCUSDT",
        "execution_symbol": "BTCUSDT",
        "side": "long",
        "quantity": 0.25,
        "entry_price": 100.0,
        "target_price": 120.0,
        "tp_order_id": "tp-1",
        "sl_order_id": "sl-should-not-be-queried",
        "exit_strategy": "take_profit_only",
    })
    kraken = FakeKraken()

    report_final_events(kraken, state)

    assert kraken.order_queries == [{"symbol": "BTCUSDT", "order_id": "tp-1"}]
    assert state.open_positions() == {}
    closed = state.closed_positions()
    assert closed[-1]["close_reason"] == "take_profit_filled"


def test_run_once_invokes_position_sync_for_position_without_take_profit(monkeypatch):
    import raspberry_executor.run_once as run_once_module

    calls = []

    class FakeSignalMaker:
        def __init__(self, *_args, **_kwargs):
            pass

        def get_open_candidates(self, limit: int) -> list[dict]:
            calls.append(("fetch", limit))
            return []

    class FakeState:
        def open_positions(self) -> dict:
            return {
                "candidate-btc": {
                    "candidate_id": "candidate-btc",
                    "execution_symbol": "BTCUSDT",
                    "tp_order_id": None,
                }
            }

    settings = SimpleNamespace(
        signalmaker_base_url="http://signalmaker.local",
        gateway_id="gateway-test",
        allowed_symbols=["USDT"],
        max_candidate_age_seconds=3600,
        exchange="fake",
    )
    exchange = SimpleNamespace(exchange_name="fake")

    monkeypatch.setattr(run_once_module, "load_settings", lambda: settings)
    monkeypatch.setattr(run_once_module, "SignalMakerClient", FakeSignalMaker)
    monkeypatch.setattr(run_once_module, "create_spot_exchange", lambda _settings: (exchange, None))
    monkeypatch.setattr(run_once_module, "StateStore", FakeState)
    monkeypatch.setattr(run_once_module, "execute_candidate", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(run_once_module.position_sync_v2, "sync_open_positions", lambda: calls.append(("sync",)) or {"missing_tp": 1})

    summary = run_once_module.run_once(limit=3)

    assert calls == [("fetch", 3), ("sync",)]
    assert summary["open_positions"] == 1


def test_main_loop_invokes_position_sync_for_position_without_take_profit(monkeypatch):
    import raspberry_executor.main as main_module

    calls = []

    class FakeSignalMaker:
        def __init__(self, *_args, **_kwargs):
            pass

        def get_open_candidates(self, limit: int) -> list[dict]:
            calls.append(("fetch", limit))
            return []

    class FakeState:
        def open_positions(self) -> dict:
            return {
                "candidate-btc": {
                    "candidate_id": "candidate-btc",
                    "execution_symbol": "BTCUSDT",
                    "tp_order_id": None,
                }
            }

    settings = SimpleNamespace(
        signalmaker_base_url="http://signalmaker.local",
        gateway_id="gateway-test",
        allowed_symbols=["USDT"],
        max_candidate_age_seconds=3600,
        exchange="fake",
        dry_run=True,
        order_quote_amount=25.0,
        poll_seconds=0,
    )
    exchange = SimpleNamespace(exchange_name="fake")

    monkeypatch.setattr(main_module, "load_settings", lambda: settings)
    monkeypatch.setattr(main_module, "SignalMakerClient", FakeSignalMaker)
    monkeypatch.setattr(main_module, "create_spot_exchange", lambda _settings: (exchange, None))
    monkeypatch.setattr(main_module, "SpotOrderManager", lambda *_args, **_kwargs: SimpleNamespace())
    monkeypatch.setattr(main_module, "StateStore", FakeState)
    monkeypatch.setattr(main_module, "execute_candidate", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module.position_sync_v2, "sync_open_positions", lambda: calls.append(("sync",)) or {"missing_tp": 1})
    monkeypatch.setattr(main_module.time, "sleep", lambda _seconds: (_ for _ in ()).throw(RuntimeError("stop loop")))

    try:
        main_module.main()
    except RuntimeError as exc:
        assert str(exc) == "stop loop"
    else:
        raise AssertionError("main loop did not stop")

    assert calls == [("fetch", 10), ("sync",)]

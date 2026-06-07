from __future__ import annotations

import raspberry_executor.sqlite_db as sqlite_db
from raspberry_executor.position_sync_v2 import _handle_filled_take_profit, _replay_take_profit
from raspberry_executor.state import StateStore


class FakeBinance:
    dry_run = False

    def __init__(self):
        self.order_queries = []

    def open_orders(self, symbol: str):
        return []

    def get_order(self, symbol: str, order_id):
        self.order_queries.append({"symbol": symbol, "order_id": order_id})
        return {"orderId": order_id, "status": "FILLED", "side": "BUY", "executedQty": "1.0"}

    def free_balance(self, asset: str) -> float:
        return 1.0


class FakeRules:
    def base_asset(self, symbol: str) -> str:
        return "BTC"


class FakeSpotManager:
    def __init__(self):
        self.calls = []

    def create_exit_take_profit_for_open_long(self, *, symbol, quantity, target_price):
        qty = float(quantity)
        self.calls.append({"symbol": symbol, "quantity": qty, "target_price": target_price})
        if qty > 0.5:
            raise RuntimeError("simulated_full_size_reject")
        return {"symbol": symbol, "quantity": str(qty), "tp_order_id": "tp-half", "tp_payload": {"orderId": "tp-half", "status": "NEW"}}


class FakeMarginManager:
    class Margin:
        def open_margin_orders(self, symbol: str):
            return []

    margin = Margin()


def state_store(tmp_path, monkeypatch) -> StateStore:
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    return StateStore()


def test_tp_replay_halves_quantity_after_full_size_failure(tmp_path, monkeypatch):
    state = state_store(tmp_path, monkeypatch)
    candidate_id = "candidate-btc"
    state.add_open_position(candidate_id, {
        "candidate_id": candidate_id,
        "signal_symbol": "BTCUSDT",
        "execution_symbol": "BTCUSDT",
        "side": "long",
        "quantity": "1.0",
        "entry_price": 100.0,
        "target_price": 120.0,
        "entry_order_id": "entry-1",
        "entry_payload": {"orderId": "entry-1", "status": "FILLED", "executedQty": "1.0"},
        "tp_order_id": None,
        "exit_strategy": "take_profit_only",
        "needs_tp_replay": True,
    })
    spot = FakeSpotManager()

    result = _replay_take_profit(candidate_id, state.open_positions()[candidate_id], "BTCUSDT", spot, FakeMarginManager(), state, binance=FakeBinance(), rules=FakeRules())

    assert result == "replayed"
    assert [call["quantity"] for call in spot.calls] == [1.0, 0.5]
    position = state.open_positions()[candidate_id]
    assert position["tp_order_id"] == "tp-half"
    assert position["tp_replay_status"] == "partial_placed"
    assert position["tp_replay_fraction"] == 0.5
    assert position["tp_protected_quantity"] == 0.5
    assert position["tp_unprotected_quantity"] == 0.5
    assert position["sl_order_id"] is None
    assert position["oco_order_list_id"] is None


def test_partial_take_profit_fill_keeps_position_open_for_replay(tmp_path, monkeypatch):
    state = state_store(tmp_path, monkeypatch)
    candidate_id = "candidate-btc"
    state.add_open_position(candidate_id, {
        "candidate_id": candidate_id,
        "signal_symbol": "BTCUSDT",
        "execution_symbol": "BTCUSDT",
        "side": "long",
        "quantity": "1.0",
        "entry_price": 100.0,
        "target_price": 120.0,
        "tp_order_id": "tp-half",
        "tp_protected_quantity": 0.5,
        "exit_strategy": "take_profit_only",
    })

    result = _handle_filled_take_profit(candidate_id, state.open_positions()[candidate_id], {"orderId": "tp-half", "status": "FILLED"}, state)

    assert result == "partial"
    position = state.open_positions()[candidate_id]
    assert float(position["quantity"]) == 0.5
    assert position["tp_order_id"] is None
    assert position["needs_tp_replay"] is True
    assert state.closed_positions() == []

from __future__ import annotations

import raspberry_executor.sqlite_db as sqlite_db
from raspberry_executor.position_sync_v2 import _handle_filled_take_profit, _replay_take_profit, _track_momentum_position
from raspberry_executor.state import StateStore


class FakeKraken:
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

    def account(self) -> dict:
        return {"balances": [{"asset": "BTC", "free": "1.0", "locked": "0"}]}


class FakeMargin:
    def __init__(self, balances):
        self.balances = balances

    def margin_free_balance(self, symbol: str, asset: str) -> float:
        balance = self.balances.get((symbol, asset), 0.0)
        if isinstance(balance, dict):
            return float(balance.get("free", 0.0))
        return balance

    def isolated_account(self, symbol: str) -> dict:
        assets = []
        for (balance_symbol, asset), balance in self.balances.items():
            if balance_symbol != symbol:
                continue
            if not isinstance(balance, dict):
                balance = {"free": balance, "locked": 0.0}
            assets.append({"asset": asset, **balance})
        return {"userAssets": assets}

    @property
    def isolated(self) -> bool:
        return False

    def open_margin_orders(self, symbol: str):
        return []


class FakeMargin:
    def __init__(self, balances):
        self.balances = balances

    def margin_free_balance(self, symbol: str, asset: str) -> float:
        return self.balances.get((symbol, asset), 0.0)


class FakeRules:
    def base_asset(self, symbol: str) -> str:
        for quote in ("USDT", "USDC", "BTC"):
            if symbol.endswith(quote):
                return symbol[: -len(quote)]
        return symbol


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

    result = _replay_take_profit(candidate_id, state.open_positions()[candidate_id], "BTCUSDT", spot, FakeMarginManager(), state, kraken=FakeKraken(), rules=FakeRules())

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


def test_track_momentum_uses_margin_balance_for_cross_margin_position(tmp_path, monkeypatch):
    state = state_store(tmp_path, monkeypatch)
    candidate_id = "momentum-tiausdc"
    position = {
        "candidate_id": candidate_id,
        "signal_symbol": "TIAUSDC",
        "execution_symbol": "TIAUSDC",
        "side": "long",
        "quantity": "250",
        "entry_price": 0.4,
        "mode": "cross_margin",
        "margin_isolated": False,
        "momentum_decision": {"buy_symbol": "TIAUSDC"},
    }
    state.add_open_position(candidate_id, position)
    kraken = FakeKraken()
    kraken.current_price = lambda symbol: 0.4
    kraken.free_balance = lambda asset: 0.0

    closed = _track_momentum_position(candidate_id, state.open_positions()[candidate_id], "TIAUSDC", kraken, FakeRules(), state, margin=FakeMargin({("TIAUSDC", "TIA"): 250.0}))

    assert closed is False
    tracked = state.open_positions()[candidate_id]
    assert tracked["balance_source"] == "margin"
    assert tracked["available_base_balance"] == 250.0
    assert state.closed_positions() == []


def test_track_momentum_defers_recent_missing_balance(tmp_path, monkeypatch):
    state = state_store(tmp_path, monkeypatch)
    monkeypatch.setenv("MOMENTUM_BALANCE_MISSING_GRACE_SECONDS", "120")
    candidate_id = "momentum-tiausdc"
    position = {
        "candidate_id": candidate_id,
        "signal_symbol": "TIAUSDC",
        "execution_symbol": "TIAUSDC",
        "side": "long",
        "quantity": "250",
        "entry_price": 0.4,
        "mode": "cross_margin",
        "margin_isolated": False,
        "momentum_decision": {"buy_symbol": "TIAUSDC"},
    }
    state.add_open_position(candidate_id, position)
    kraken = FakeKraken()
    kraken.current_price = lambda symbol: 0.4
    kraken.free_balance = lambda asset: 0.0

    closed = _track_momentum_position(candidate_id, state.open_positions()[candidate_id], "TIAUSDC", kraken, FakeRules(), state, margin=FakeMargin({("TIAUSDC", "TIA"): 0.0}))

    assert closed is False
    tracked = state.open_positions()[candidate_id]
    assert tracked["balance_missing_grace_until_age_seconds"] == 120.0
    assert state.closed_positions() == []
    assert any(event["event_type"] == "momentum_balance_missing_grace" for event in state.events())

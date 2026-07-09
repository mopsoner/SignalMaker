from __future__ import annotations

from types import SimpleNamespace

import raspberry_executor.candidate_levels as candidate_levels
import raspberry_executor.sqlite_db as sqlite_db
from raspberry_executor.position_sync_v2 import _replay_take_profit
from raspberry_executor.state import StateStore


class FakeSignalMakerClient:
    def __init__(self, base_url: str, gateway_id: str):
        self.base_url = base_url
        self.gateway_id = gateway_id

    def get_recent_candidates(self, symbol: str, limit: int) -> list[dict]:
        return [
            {
                "candidate_id": "candidate-btc",
                "symbol": "BTCUSDT",
                "target_price": 120.0,
                "created_at": "2026-07-09T00:00:00Z",
            }
        ]


class FakeRules:
    def base_asset(self, symbol: str) -> str:
        return "BTC"


class FakeKraken:
    dry_run = False

    def open_orders(self, symbol: str) -> list[dict]:
        return []

    def get_order(self, symbol: str, order_id: str) -> dict:
        return {"orderId": order_id, "status": "FILLED", "executedQty": "1.0"}

    def free_balance(self, asset: str) -> float:
        return 1.0


class FakeSpotManager:
    def __init__(self):
        self.calls = []

    def create_exit_take_profit_for_open_long(self, *, symbol, quantity, target_price):
        self.calls.append({"symbol": symbol, "quantity": float(quantity), "target_price": target_price})
        return {"quantity": str(quantity), "tp_order_id": "tp-1", "tp_payload": {"orderId": "tp-1", "status": "NEW"}}


class FakeMarginManager:
    class Margin:
        def open_margin_orders(self, symbol: str) -> list[dict]:
            return []

    margin = Margin()


def state_store(tmp_path, monkeypatch) -> StateStore:
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    return StateStore()


def test_latest_levels_accepts_take_profit_without_stop(monkeypatch):
    monkeypatch.setattr(candidate_levels, "load_settings", lambda: SimpleNamespace(signalmaker_base_url="http://signalmaker.local", gateway_id="gateway-test"))
    monkeypatch.setattr(candidate_levels, "SignalMakerClient", FakeSignalMakerClient)

    levels = candidate_levels.latest_levels_for_symbol("BTCUSDT")

    assert levels is not None
    assert levels["target_price"] == 120.0
    assert levels["stop_price"] is None
    assert levels["source_candidate_id"] == "candidate-btc"


def test_tp_replay_uses_latest_take_profit_level_without_stop(tmp_path, monkeypatch):
    import raspberry_executor.position_sync_v2 as position_sync_v2

    monkeypatch.setattr(position_sync_v2, "latest_levels_for_symbol", lambda symbol: {"target_price": 120.0, "stop_price": None, "source": "signalmaker_recent_candidate"})
    state = state_store(tmp_path, monkeypatch)
    candidate_id = "candidate-btc"
    state.add_open_position(candidate_id, {
        "candidate_id": candidate_id,
        "signal_symbol": "BTCUSDT",
        "execution_symbol": "BTCUSDT",
        "side": "long",
        "quantity": "1.0",
        "entry_price": 100.0,
        "entry_order_id": "entry-1",
        "entry_payload": {"orderId": "entry-1", "status": "FILLED", "executedQty": "1.0"},
        "tp_order_id": None,
        "exit_strategy": "take_profit_only",
        "needs_tp_replay": True,
    })
    spot = FakeSpotManager()

    result = _replay_take_profit(candidate_id, state.open_positions()[candidate_id], "BTCUSDT", spot, FakeMarginManager(), state, kraken=FakeKraken(), rules=FakeRules())

    assert result == "replayed"
    assert spot.calls == [{"symbol": "BTCUSDT", "quantity": 1.0, "target_price": 120.0}]
    position = state.open_positions()[candidate_id]
    assert position["target_price"] == 120.0
    assert position["tp_order_id"] == "tp-1"
    assert position["sl_order_id"] is None

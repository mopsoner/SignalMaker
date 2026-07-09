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

    def get_recent_candidates(self, symbol: str, limit: int, use_cursor: bool = True) -> list[dict]:
        return [
            {
                "candidate_id": "candidate-btc",
                "symbol": "BTCUSDT",
                "target_price": 120.0,
                "created_at": "2026-07-09T00:00:00Z",
            }
        ]


class FakeSignalMakerClientMultipleCandidates:
    def __init__(self, base_url: str, gateway_id: str):
        self.base_url = base_url
        self.gateway_id = gateway_id

    def get_recent_candidates(self, symbol: str, limit: int, use_cursor: bool = True) -> list[dict]:
        return [
            {
                "candidate_id": "older-candidate",
                "remote_candidate_id": "remote-older",
                "signal_fingerprint": "fingerprint-older",
                "symbol": "BTCUSDT",
                "entry_price": 100.0,
                "target_price": 111.0,
                "created_at": "2026-07-08T00:00:00Z",
            },
            {
                "candidate_id": "newer-candidate",
                "remote_candidate_id": "remote-newer",
                "signal_fingerprint": "fingerprint-newer",
                "symbol": "BTCUSDT",
                "entry_price": 101.0,
                "target_price": 130.0,
                "created_at": "2026-07-09T00:00:00Z",
            },
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

    monkeypatch.setattr(position_sync_v2, "levels_for_position", lambda position, symbol: {"target_price": 120.0, "stop_price": None, "source": "signalmaker_recent_candidate"})
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


def test_tp_replay_uses_matched_older_candidate_instead_of_latest(tmp_path, monkeypatch):
    monkeypatch.setattr(candidate_levels, "load_settings", lambda: SimpleNamespace(signalmaker_base_url="http://signalmaker.local", gateway_id="gateway-test"))
    monkeypatch.setattr(candidate_levels, "SignalMakerClient", FakeSignalMakerClientMultipleCandidates)
    state = state_store(tmp_path, monkeypatch)
    candidate_id = "position-candidate"
    state.add_open_position(candidate_id, {
        "candidate_id": "older-candidate",
        "remote_candidate_id": "remote-older",
        "signal_fingerprint": "fingerprint-older",
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
    assert spot.calls == [{"symbol": "BTCUSDT", "quantity": 1.0, "target_price": 111.0}]
    position = state.open_positions()[candidate_id]
    assert position["target_price"] == 111.0
    assert position["tp_replay_level_source"]["source"] == "signalmaker_matched_candidate"
    assert position["tp_replay_level_source"]["source_candidate_id"] == "older-candidate"


def test_repair_candidate_lookup_ignores_advanced_cursor(tmp_path, monkeypatch):
    from raspberry_executor.candidate_cursor_store import read_candidate_cursor, set_candidate_cursor
    from raspberry_executor.signalmaker_client import SignalMakerClient

    class FakeResponse:
        def __init__(self, payload):
            self.payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self.payload

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, params=None, timeout=None):
            self.calls.append({"url": url, "params": dict(params or {}), "timeout": timeout})
            return FakeResponse([
                {
                    "candidate_id": "old-remote-candidate",
                    "symbol": "BTCUSDT",
                    "side": "buy",
                    "entry_price": 100.0,
                    "target_price": 125.0,
                    "created_at": "2026-07-08T00:00:00+00:00",
                }
            ])

    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    sqlite_db.init_db()
    advanced_cursor = set_candidate_cursor("2026-07-09T00:00:00+00:00")

    client = SignalMakerClient("http://signalmaker.local", "gateway-test")
    fake_session = FakeSession()
    client.session = fake_session

    candidates = client.get_candidates_for_repair("BTCUSDT", limit=100)

    assert fake_session.calls == [
        {
            "url": "http://signalmaker.local/api/v1/trade-candidates",
            "params": {"limit": 100, "symbol": "BTCUSDT"},
            "timeout": 15,
        }
    ]
    assert read_candidate_cursor() == advanced_cursor
    assert len(candidates) == 1
    assert candidates[0]["remote_candidate_id"] == "old-remote-candidate"
    assert candidates[0]["target_price"] == 125.0

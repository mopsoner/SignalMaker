from types import SimpleNamespace

from raspberry_executor import classic_candidate_executor
from raspberry_executor.margin_executor import process_candidate
from raspberry_executor.risk_guard import RiskGuard
from raspberry_executor.state import StateStore


class FakeRules:
    def symbol_info(self, symbol):
        return {"quoteAsset": "USD"}


class FakeExchange:
    exchange_name = "kraken"
    dry_run = False


class RecordingMarginManager:
    def __init__(self, *, fail_with: str | None = None):
        self.fail_with = fail_with
        self.calls = []

    def open_long_with_margin_take_profit(self, *, symbol, quote_amount, target_price, leverage=None):
        self.calls.append({
            "symbol": symbol,
            "quote_amount": quote_amount,
            "target_price": target_price,
            "leverage": leverage,
        })
        if self.fail_with:
            raise RuntimeError(self.fail_with)
        return {
            "symbol": symbol,
            "side": "long",
            "quantity": "0.2",
            "entry_price": 100.0,
            "entry_order_id": "margin-entry-1",
            "tp_order_id": "margin-tp-1",
            "entry_payload": {
                "orderId": "margin-entry-1",
                "symbol": symbol,
                "side": "BUY",
                "type": "MARKET",
                "margin": True,
                "leverage": str(leverage),
            },
            "tp_payload": {
                "orderId": "margin-tp-1",
                "symbol": symbol,
                "side": "SELL",
                "type": "LIMIT",
                "margin": True,
                "price": str(target_price),
            },
        }


class RecordingSpotManager:
    def __init__(self, *, fail_with: str | None = None):
        self.fail_with = fail_with
        self.calls = []

    def open_long_with_take_profit(self, *, symbol, quote_amount, target_price):
        self.calls.append({
            "symbol": symbol,
            "quote_amount": quote_amount,
            "target_price": target_price,
        })
        if self.fail_with:
            raise RuntimeError(self.fail_with)
        return {
            "symbol": symbol,
            "side": "long",
            "quantity": "0.2",
            "entry_price": 100.0,
            "entry_order_id": "spot-entry-1",
            "tp_order_id": "spot-tp-1",
            "entry_payload": {
                "orderId": "spot-entry-1",
                "symbol": symbol,
                "side": "BUY",
                "type": "MARKET",
            },
            "tp_payload": {
                "orderId": "spot-tp-1",
                "symbol": symbol,
                "side": "SELL",
                "type": "LIMIT",
                "price": str(target_price),
            },
        }


_SETTINGS = SimpleNamespace(order_quote_amount=20.0, exchange="kraken")


def _candidate(candidate_id="candidate-1"):
    return {
        "candidate_id": candidate_id,
        "symbol": "BTCUSD",
        "side": "long",
        "status": "open",
        "entry_price": 100.0,
        "target_price": 110.0,
        "stop_price": 95.0,
    }


def _state(tmp_path, monkeypatch):
    from raspberry_executor import sqlite_db

    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "state.db")
    return StateStore()


def _patch_orchestration_deps(monkeypatch):
    monkeypatch.setattr(classic_candidate_executor, "margin_enabled", lambda: True)
    monkeypatch.setattr(classic_candidate_executor, "margin_leverage_attempts", lambda: (5,))
    monkeypatch.setattr(classic_candidate_executor, "upsert_remote_candidates", lambda candidates: None)
    monkeypatch.setattr(classic_candidate_executor, "mark_candidate_executed", lambda candidate_id: None)
    monkeypatch.setattr(classic_candidate_executor, "remove_pending", lambda candidate_id: None)


def _events_for(state, candidate_id):
    return [event for event in state.events() if event["candidate_id"] == candidate_id]


def test_process_candidate_long_margin_available_opens_margin_tp_without_spot_fallback(tmp_path, monkeypatch):
    _patch_orchestration_deps(monkeypatch)
    state = _state(tmp_path, monkeypatch)
    margin_manager = RecordingMarginManager()
    spot_manager = RecordingSpotManager()

    result = process_candidate(
        _SETTINGS, FakeExchange(), FakeRules(), margin_manager, spot_manager, state, RiskGuard(["USD"], 999999), _candidate("margin-ok")
    )

    assert result == "opened"
    assert margin_manager.calls == [{"symbol": "BTCUSD", "quote_amount": 20.0, "target_price": 110.0, "leverage": 5}]
    assert spot_manager.calls == []
    position = state.open_positions()["margin-ok"]
    assert position["mode"] == "cross_margin"
    assert position["entry_payload"] == {
        "orderId": "margin-entry-1",
        "symbol": "BTCUSD",
        "side": "BUY",
        "type": "MARKET",
        "margin": True,
        "leverage": "5",
    }
    assert position["tp_payload"]["side"] == "SELL"
    assert position["tp_payload"]["type"] == "LIMIT"
    assert not any(event["event_type"] == "candidate_margin_fallback_spot" for event in _events_for(state, "margin-ok"))


def test_process_candidate_long_margin_rejected_falls_back_to_spot_without_margin_only_fields(tmp_path, monkeypatch):
    _patch_orchestration_deps(monkeypatch)
    state = _state(tmp_path, monkeypatch)
    margin_manager = RecordingMarginManager(fail_with="margin unavailable for pair")
    spot_manager = RecordingSpotManager()

    result = process_candidate(
        _SETTINGS, FakeExchange(), FakeRules(), margin_manager, spot_manager, state, RiskGuard(["USD"], 999999), _candidate("spot-fallback")
    )

    assert result == "opened_spot_fallback"
    assert margin_manager.calls == [{"symbol": "BTCUSD", "quote_amount": 20.0, "target_price": 110.0, "leverage": 5}]
    assert spot_manager.calls == [{"symbol": "BTCUSD", "quote_amount": 20.0, "target_price": 110.0}]
    position = state.open_positions()["spot-fallback"]
    assert position["mode"] == "spot"
    assert position["entry_payload"]["side"] == "BUY"
    assert position["entry_payload"]["type"] == "MARKET"
    assert position["tp_payload"]["side"] == "SELL"
    assert position["tp_payload"]["type"] == "LIMIT"
    assert "leverage" not in position["entry_payload"]
    assert "reduce_only" not in position["entry_payload"]
    assert "leverage" not in position["tp_payload"]
    assert "reduce_only" not in position["tp_payload"]
    events = _events_for(state, "spot-fallback")
    assert [event["event_type"] for event in events if event["event_type"].startswith("candidate_margin")] == [
        "candidate_margin_attempt_failed",
        "candidate_margin_fallback_spot",
    ]


def test_process_candidate_long_margin_and_spot_fail_records_clear_errors(tmp_path, monkeypatch):
    _patch_orchestration_deps(monkeypatch)
    state = _state(tmp_path, monkeypatch)
    margin_manager = RecordingMarginManager(fail_with="margin unavailable for pair")
    spot_manager = RecordingSpotManager(fail_with="spot rejected: insufficient funds")

    result = process_candidate(
        _SETTINGS, FakeExchange(), FakeRules(), margin_manager, spot_manager, state, RiskGuard(["USD"], 999999), _candidate("all-fail")
    )

    assert result == "error"
    assert state.open_positions() == {}
    events = _events_for(state, "all-fail")
    event_types = [event["event_type"] for event in events]
    assert "candidate_margin_attempt_failed" in event_types
    assert "candidate_margin_fallback_spot" in event_types
    assert "candidate_spot_fallback_failed" in event_types
    assert "execution_error" in event_types
    spot_failure = next(event for event in events if event["event_type"] == "candidate_spot_fallback_failed")
    assert spot_failure["payload"]["error"] == "spot rejected: insufficient funds"
    assert spot_failure["payload"]["margin_error"] == "margin unavailable for pair"
    execution_error = next(event for event in events if event["event_type"] == "execution_error")
    assert execution_error["payload"]["error"] == "spot rejected: insufficient funds"
    assert execution_error["payload"]["margin_error"] == "margin unavailable for pair"

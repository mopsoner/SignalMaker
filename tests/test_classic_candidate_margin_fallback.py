from __future__ import annotations

from types import SimpleNamespace

import raspberry_executor.classic_candidate_executor as executor
from raspberry_executor.risk_guard import RiskGuard


class FakeState:
    def __init__(self):
        self.events_log = []
        self.positions = {}
        self.executed = set()
        self.fingerprints = set()

    def already_executed(self, candidate_id: str) -> bool:
        return candidate_id in self.executed

    def already_executed_fingerprint(self, fingerprint: str) -> bool:
        return fingerprint in self.fingerprints

    def has_open_position_for(self, symbol: str, side: str | None = None) -> bool:
        return False

    def mark_executed(self, candidate_id: str) -> None:
        self.executed.add(candidate_id)

    def mark_executed_fingerprint(self, fingerprint: str) -> None:
        self.fingerprints.add(fingerprint)

    def add_open_position(self, candidate_id: str, payload: dict) -> None:
        self.positions[candidate_id] = payload
        self.add_event(candidate_id, "position_opened", payload)

    def add_event(self, candidate_id: str, event_type: str, payload: dict | None = None) -> None:
        self.events_log.append((candidate_id, event_type, payload or {}))


class FakeRules:
    def symbol_info(self, symbol: str) -> dict:
        return {"quoteAsset": "USDC"}


class FakeExchange:
    exchange_name = "kraken"
    dry_run = False

    def __init__(self, free_quote: float = 100.0):
        self.free_quote = free_quote

    def free_balance(self, asset: str) -> float:
        assert asset == "USDC"
        return self.free_quote


class FailingMarginManager:
    def __init__(self, error: str):
        self.error = error

    def open_long_with_margin_take_profit(self, **kwargs):
        raise RuntimeError(self.error)


class FakeSpotManager:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.calls = []

    def open_long_with_take_profit(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail:
            raise RuntimeError("spot rejected")
        return {
            "quantity": "1.0",
            "entry_price": 10.0,
            "entry_order_id": "spot-entry-1",
            "tp_order_id": "spot-tp-1",
            "entry_payload": {"side": "BUY", "type": "MARKET"},
            "tp_payload": {"side": "SELL", "type": "LIMIT"},
        }


def candidate(candidate_id: str = "cand-1") -> dict:
    return {"candidate_id": candidate_id, "status": "open", "symbol": "BTCUSDC", "side": "long", "entry_price": 10.0, "target_price": 12.0}


def run_candidate(monkeypatch, margin_error: str, *, spot_free: float = 100.0, spot_fail: bool = False):
    monkeypatch.setattr(executor, "margin_enabled", lambda: True)
    monkeypatch.setattr(executor, "_ensure_candidate_visible", lambda candidate: None)
    monkeypatch.setattr(executor, "_mark_remote_executed", lambda candidate_id: None)
    monkeypatch.setattr(executor, "remove_pending", lambda candidate_id: None)
    state = FakeState()
    spot = FakeSpotManager(fail=spot_fail)
    result = executor.execute_classic_candidate(
        SimpleNamespace(order_quote_amount=20.0, exchange="kraken"),
        FakeExchange(free_quote=spot_free),
        FakeRules(),
        FailingMarginManager(margin_error),
        spot,
        state,
        RiskGuard(["USDC"], 999999),
        candidate(),
    )
    return result, state, spot


def test_recoverable_margin_unavailable_falls_back_to_spot(monkeypatch):
    result, state, spot = run_candidate(monkeypatch, "symbol not supported for margin")

    assert result == "opened_spot_fallback"
    assert len(spot.calls) == 1
    assert state.positions["cand-1"]["mode"] == "spot"
    assert [event for _, event, _ in state.events_log] == [
        "candidate_margin_attempt_failed",
        "candidate_margin_fallback_spot",
        "position_opened",
    ]


def test_margin_insufficient_balance_requires_spot_quote_before_fallback(monkeypatch):
    result, state, spot = run_candidate(monkeypatch, "margin_insufficient_quote_balance", spot_free=0.0)

    assert result == "error"
    assert spot.calls == []
    assert [event for _, event, _ in state.events_log] == [
        "candidate_margin_attempt_failed",
        "execution_error",
    ]


def test_recoverable_margin_spot_fallback_failure_is_explicit(monkeypatch):
    result, state, spot = run_candidate(monkeypatch, "leverage unavailable", spot_fail=True)

    assert result == "error"
    assert len(spot.calls) == 1
    assert [event for _, event, _ in state.events_log] == [
        "candidate_margin_attempt_failed",
        "candidate_margin_fallback_spot",
        "candidate_spot_fallback_failed",
        "execution_error",
    ]

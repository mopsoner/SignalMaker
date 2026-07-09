from __future__ import annotations

import raspberry_executor.sqlite_db as sqlite_db
from raspberry_executor.candidate_status_sync import _protected, sync_executed_candidates
from raspberry_executor.state import StateStore


def state_store(tmp_path, monkeypatch) -> StateStore:
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    return StateStore()


def test_protected_requires_exchange_confirmed_take_profit():
    assert not _protected({"tp_order_id": "tp-1"})
    assert not _protected({"tp_order_id": "tp-1", "execution_symbol": "BTCUSDT", "quantity": "1"})
    assert _protected({"tp_order_id": "tp-1", "execution_symbol": "BTCUSDT", "quantity": "1", "tp_payload": {"status": "NEW"}})


def test_sync_defers_local_tp_without_exchange_confirmation(tmp_path, monkeypatch):
    state = state_store(tmp_path, monkeypatch)
    candidate_id = "candidate-local-tp"
    state.add_open_position(candidate_id, {
        "candidate_id": candidate_id,
        "signal_symbol": "BTCUSDT",
        "execution_symbol": "BTCUSDT",
        "side": "long",
        "quantity": "1.0",
        "entry_order_id": "entry-1",
        "tp_order_id": "tp-1",
        "tp_payload": {"orderId": "tp-1"},
    })

    summary = sync_executed_candidates()

    assert summary["marked"] == 0
    assert summary["protected"] == 0
    assert not state.already_executed(candidate_id)
    events = state.events()
    assert events[-1]["event_type"] == "candidate_tp_unconfirmed"
    assert events[-1]["payload"]["local_tp_recorded"] is True
    assert events[-1]["payload"]["exchange_tp_confirmed"] is False
    assert events[-1]["payload"]["position_exists_on_asset"] is True


def test_sync_marks_candidate_with_confirmed_exchange_tp(tmp_path, monkeypatch):
    state = state_store(tmp_path, monkeypatch)
    candidate_id = "candidate-confirmed-tp"
    state.add_open_position(candidate_id, {
        "candidate_id": candidate_id,
        "signal_symbol": "ETHUSDT",
        "execution_symbol": "ETHUSDT",
        "side": "long",
        "quantity": "2.0",
        "entry_order_id": "entry-2",
        "tp_order_id": "tp-2",
        "tp_exchange_status": "OPEN",
        "tp_payload": {"orderId": "tp-2"},
    })

    summary = sync_executed_candidates()

    assert summary["marked"] == 1
    assert summary["protected"] == 1
    assert state.already_executed(candidate_id)
    position = state.open_positions()[candidate_id]
    assert position["local_candidate_status"] == "executed"
    assert position["local_candidate_executed_reason"] == "position_has_confirmed_take_profit"
    assert position["local_tp_recorded"] is True
    assert position["exchange_tp_confirmed"] is True
    assert position["position_exists_on_asset"] is True

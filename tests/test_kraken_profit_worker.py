import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.core.config import Settings
from scripts import run_kraken_profit_loop as worker
from app.services import wyckoff_goal_service as goal


def test_enabled_by_default():
    assert Settings(_env_file=None).kraken_profit_worker_enabled is True


def test_disabled_worker_does_not_open_session(monkeypatch):
    monkeypatch.setattr(worker, "settings", SimpleNamespace(kraken_profit_worker_enabled=False))
    factory = MagicMock()
    assert worker.run_once(factory)["enabled"] is False
    factory.assert_not_called()


def test_enabled_tick_commits_and_closes_its_session(monkeypatch):
    monkeypatch.setattr(worker, "settings", SimpleNamespace(kraken_profit_worker_enabled=True))
    context = MagicMock()
    db = context.__enter__.return_value
    service = MagicMock()
    service.run.return_value = {"enabled": True}
    constructor = MagicMock(return_value=service)
    monkeypatch.setattr(worker, "WyckoffGoalService", constructor)
    assert worker.run_once(lambda: context) == {"enabled": True}
    constructor.assert_called_once_with(db)
    db.commit.assert_called_once()
    context.__exit__.assert_called_once()


def test_loop_recovers_from_tick_failure(monkeypatch):
    monkeypatch.setattr(worker, "settings", SimpleNamespace(
        kraken_profit_worker_enabled=True, kraken_profit_worker_interval_seconds=2))
    monkeypatch.setattr(sys, "argv", ["profit-worker"])
    run = MagicMock(side_effect=[RuntimeError("Kraken unavailable"), {"enabled": True}])
    monkeypatch.setattr(worker, "run_once", run)
    beat = MagicMock()
    monkeypatch.setattr(worker, "heartbeat", beat)
    monkeypatch.setattr(worker.time, "sleep", MagicMock(side_effect=[None, KeyboardInterrupt]))
    with pytest.raises(KeyboardInterrupt):
        worker.main()
    assert run.call_count == 2
    assert beat.call_args_list[0].kwargs["state"] == "error"
    assert beat.call_args_list[1].kwargs["state"] == "running"


def test_disabled_service_cannot_submit_even_via_reconcile(monkeypatch):
    monkeypatch.setattr(goal.settings, "kraken_profit_worker_enabled", False)
    service = goal.WyckoffGoalService(None, object())
    monkeypatch.setattr(service, "summary", lambda: {})
    assert service.run()["enabled"] is False


@pytest.mark.parametrize("interval", [0, -1, float("inf")])
def test_invalid_polling_interval_rejected(interval):
    with pytest.raises(ValueError):
        Settings(_env_file=None, KRAKEN_PROFIT_WORKER_INTERVAL_SECONDS=interval)


@pytest.mark.parametrize("enabled,status,meta,blocked", [
    (False, "open", {}, False),
    (True, "open", {}, True),
    (False, "closing", {}, True),
    (False, "open", {"goal_triggered": True}, True),
    (True, "open", {"goal_verified_at": "2026-10-13T21:00:00Z"}, False),
])
def test_entry_guard_reads_local_state_only(monkeypatch, enabled, status, meta, blocked):
    monkeypatch.setattr(goal.settings, "kraken_profit_worker_enabled", enabled)
    service = goal.WyckoffGoalService(None, object())
    monkeypatch.setattr(service, "rows", lambda: [SimpleNamespace(status=status, meta=meta)])
    assert service.entry_blocked() == blocked

from __future__ import annotations

import fcntl

import raspberry_executor.position_sync_v2 as sync_module
import raspberry_executor.sqlite_db as sqlite_db
from raspberry_executor import ui_contract


def _isolated_runtime(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "executor.db")
    monkeypatch.setenv("POSITION_SYNC_MIN_INTERVAL_SECONDS", "60")


def test_position_sync_skips_when_another_process_holds_lock(tmp_path, monkeypatch):
    _isolated_runtime(tmp_path, monkeypatch)
    calls = []
    monkeypatch.setattr(sync_module, "_sync_open_positions", lambda: calls.append(True) or {})
    lock_path = sync_module._position_sync_runtime_path("lock")

    with lock_path.open("a+") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = sync_module.sync_open_positions()

    assert result == {"status": "skipped", "reason": "sync_already_running"}
    assert calls == []


def test_position_sync_enforces_minimum_interval_and_force_bypasses_it(tmp_path, monkeypatch):
    _isolated_runtime(tmp_path, monkeypatch)
    calls = []
    monkeypatch.setattr(sync_module, "_sync_open_positions", lambda: calls.append(True) or {"checked": 0})

    assert sync_module.sync_open_positions() == {"checked": 0}
    skipped = sync_module.sync_open_positions()
    assert skipped["status"] == "skipped"
    assert skipped["reason"] == "sync_min_interval"
    assert sync_module.sync_open_positions(force=True) == {"checked": 0}
    assert calls == [True, True]


def test_positions_view_is_read_only_by_default(tmp_path, monkeypatch):
    _isolated_runtime(tmp_path, monkeypatch)
    monkeypatch.setattr(ui_contract, "sync_open_positions", lambda: (_ for _ in ()).throw(AssertionError("unexpected sync")))

    payload = ui_contract.positions_view()

    assert payload["sync"] == {}
    assert payload["sync_error"] == ""

from __future__ import annotations

import json

import raspberry_executor.sqlite_db as sqlite_db
from raspberry_executor.state import StateStore
from raspberry_executor.web_dashboard import events_page
from raspberry_executor.web_local import events_html


def state_store(tmp_path, monkeypatch) -> StateStore:
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    return StateStore()


def test_state_events_returns_recent_rows_after_default_limit(tmp_path, monkeypatch):
    state = state_store(tmp_path, monkeypatch)

    for index in range(1005):
        state.add_event(f"candidate-{index}", f"event-{index}", {"index": index})

    rows = state.events()

    assert len(rows) == 1000
    assert rows[0]["event_type"] == "event-5"
    assert rows[-1]["event_type"] == "event-1004"


def test_event_views_use_latest_rows_not_oldest_rows(tmp_path, monkeypatch):
    state = state_store(tmp_path, monkeypatch)

    for index in range(260):
        state.add_event(f"candidate-{index}", f"event-{index}", {"index": index})

    full_web = events_page(limit=10)
    local_web = events_html(limit=10)

    assert "event-259" in full_web
    assert "event-250" in full_web
    assert "event-249" not in full_web
    assert "event-259" in local_web
    assert "event-250" in local_web
    assert "event-249" not in local_web


def test_local_event_api_payload_is_newest_first(tmp_path, monkeypatch):
    state = state_store(tmp_path, monkeypatch)

    for index in range(3):
        state.add_event(f"candidate-{index}", f"event-{index}", {"index": index})

    payload = {"events": list(reversed(StateStore().events(limit=2))), "limit": 2}

    assert json.loads(json.dumps(payload))["events"][0]["event_type"] == "event-2"
    assert payload["events"][1]["event_type"] == "event-1"

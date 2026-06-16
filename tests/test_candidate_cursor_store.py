from __future__ import annotations

from datetime import datetime, timezone

import raspberry_executor.sqlite_db as sqlite_db
from raspberry_executor.candidate_cursor_store import (
    filter_candidates_after_cursor,
    read_candidate_cursor,
    read_last_runtime_reset_at,
    set_candidate_cursor,
)


def test_runtime_reset_timestamp_does_not_become_candidate_cursor(monkeypatch, tmp_path):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    sqlite_db.init_db()

    reset_at = datetime(2026, 6, 16, 12, 0, tzinfo=timezone.utc).isoformat()
    with sqlite_db.connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('local_runtime_data_reset_at', ?)",
            (reset_at,),
        )
        conn.commit()

    assert read_last_runtime_reset_at() == reset_at
    assert read_candidate_cursor() is None

    candidates = [{"candidate_id": "remote-1", "created_at": "2026-06-16T11:59:00+00:00"}]
    assert filter_candidates_after_cursor(candidates, read_candidate_cursor()) == candidates


def test_candidate_cursor_filters_seen_candidates(monkeypatch, tmp_path):
    monkeypatch.setattr(sqlite_db, "DB_PATH", tmp_path / "raspberry_executor.db")
    sqlite_db.init_db()
    cursor = set_candidate_cursor("2026-06-16T12:00:00+00:00")

    candidates = [
        {"candidate_id": "old", "created_at": "2026-06-16T11:59:00+00:00"},
        {"candidate_id": "new", "created_at": "2026-06-16T12:01:00+00:00"},
    ]

    assert read_candidate_cursor() == cursor
    assert filter_candidates_after_cursor(candidates, read_candidate_cursor()) == [candidates[1]]

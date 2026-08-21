import os
from pathlib import Path

import pytest
from fastapi import HTTPException

from app.api.routes import admin_settings
from app.services.worker_control_service import WorkerControlService


EXPECTED_WORKERS = {
    "pipeline",
    "wyckoff_paper",
    "scheduler",
    "momentum_paper",
    "momentum_live",
    "wyckoff_live",
    "kraken_candle_feed",
    "ibkr_ingestion",
    "stock_etf_analysis",
}


@pytest.mark.parametrize("worker_name", sorted(EXPECTED_WORKERS))
def test_operations_ui_worker_logs_are_allowed(monkeypatch, tmp_path: Path, worker_name: str):
    monkeypatch.setenv("SIGNALMAKER_LOG_DIR", str(tmp_path / "logs"))

    result = admin_settings.get_worker_logs(worker_name, lines=300)

    assert result == {
        "worker": worker_name,
        "requested_worker_id": worker_name,
        "canonical_worker_id": worker_name,
        "deprecated_alias": False,
        "path": None,
        "lines": [],
        "size_bytes": 0,
    }


def test_log_allowlist_matches_every_worker_control_service_worker():
    assert set(WorkerControlService.WORKERS) == EXPECTED_WORKERS
    assert admin_settings._ALLOWED_LOG_WORKERS == EXPECTED_WORKERS | {"application"}


@pytest.mark.parametrize(
    ("legacy_name", "canonical_name"),
    [("executor", "wyckoff_paper"), ("momentum_engine", "momentum_paper")],
)
def test_legacy_worker_log_names_resolve_to_paper_workers(
    monkeypatch, tmp_path: Path, legacy_name: str, canonical_name: str
):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_file = log_dir / f"{canonical_name}.log"
    log_file.write_text(f"{canonical_name} output\n", encoding="utf-8")
    monkeypatch.setenv("SIGNALMAKER_LOG_DIR", str(log_dir))

    result = admin_settings.get_worker_logs(legacy_name, lines=300)

    assert result["worker"] == canonical_name
    assert result["requested_worker_id"] == legacy_name
    assert result["canonical_worker_id"] == canonical_name
    assert result["deprecated_alias"] is True
    assert result["canonical_worker_id"] not in {"wyckoff_live", "momentum_live"}
    assert result["path"] == str(log_file)
    assert result["lines"] == [f"{canonical_name} output"]


def test_application_error_log_is_available(monkeypatch, tmp_path: Path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_file = log_dir / "application.log"
    log_file.write_text("2026-08-18 ERROR request_failed request_id=abc\n", encoding="utf-8")
    monkeypatch.setenv("SIGNALMAKER_LOG_DIR", str(log_dir))

    result = admin_settings.get_worker_logs("application", lines=20)

    assert result["path"] == str(log_file)
    assert result["lines"] == ["2026-08-18 ERROR request_failed request_id=abc"]


def test_worker_logs_respect_line_limit(monkeypatch, tmp_path: Path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_file = log_dir / "pipeline.log"
    log_file.write_text("first\nsecond\nthird\nfourth\n", encoding="utf-8")
    monkeypatch.setenv("SIGNALMAKER_LOG_DIR", str(log_dir))

    result = admin_settings.get_worker_logs("pipeline", lines=2)

    assert result["lines"] == ["third", "fourth"]
    assert result["path"] == str(log_file)
    assert result["size_bytes"] == log_file.stat().st_size


def test_worker_logs_choose_most_recent_candidate(monkeypatch, tmp_path: Path):
    log_dir = tmp_path / "logs"
    legacy_dir = tmp_path / ".runtime"
    log_dir.mkdir()
    legacy_dir.mkdir()
    old_empty_log = log_dir / "wyckoff_paper.log"
    recent_active_log = legacy_dir / "wyckoff_paper.log"
    old_empty_log.touch()
    recent_active_log.write_text("active worker output\n", encoding="utf-8")
    os.utime(old_empty_log, (1, 1))
    os.utime(recent_active_log, (2, 2))
    monkeypatch.setenv("SIGNALMAKER_LOG_DIR", str(log_dir))

    result = admin_settings.get_worker_logs("wyckoff_paper", lines=20)

    assert result["path"] == str(recent_active_log)
    assert result["lines"] == ["active worker output"]


def test_unknown_worker_log_is_rejected():
    with pytest.raises(HTTPException) as exc_info:
        admin_settings.get_worker_logs("../secret", lines=300)

    assert exc_info.value.status_code == 400

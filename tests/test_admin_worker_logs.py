from pathlib import Path

import pytest
from fastapi import HTTPException

from app.api.routes import admin_settings


@pytest.mark.parametrize(
    "worker_name",
    ["pipeline", "executor", "scheduler", "momentum_engine", "momentum_backtest"],
)
def test_operations_ui_worker_logs_are_allowed(monkeypatch, tmp_path: Path, worker_name: str):
    monkeypatch.setattr(admin_settings, "_ROOT", str(tmp_path))

    result = admin_settings.get_worker_logs(worker_name, lines=300)

    assert result == {"worker": worker_name, "path": None, "lines": [], "size_bytes": 0}


def test_unknown_worker_log_is_rejected():
    with pytest.raises(HTTPException) as exc_info:
        admin_settings.get_worker_logs("../secret", lines=300)

    assert exc_info.value.status_code == 400

import asyncio
from unittest.mock import ANY, Mock

import pytest

from app.services import worker_control_service as control
from signalmaker.market_data.services import MarketAnalysisJobConsumer


def test_only_exact_stable_worker_names_are_allowed(tmp_path, monkeypatch):
    monkeypatch.setattr(control, "RUNTIME_DIR", tmp_path)
    service = control.WorkerControlService()
    assert set(control.WORKERS) == {
        "pipeline",
        "wyckoff_paper",
        "kraken_candle_feed",
        "momentum_paper",
        "momentum_live",
        "wyckoff_live",
        "ibkr_ingestion",
        "stock_etf_analysis",
        "scheduler",
    }
    for neighboring_name in ("crypto_scheduler", "scheduler_crypto", "stock_etf_analysis_crypto", "stock"):
        with pytest.raises(ValueError, match="Unknown worker"):
            service.start(neighboring_name)


@pytest.mark.parametrize(
    ("legacy_name", "canonical_name", "module"),
    [
        ("executor", "wyckoff_paper", "scripts.run_wyckoff_paper_loop"),
        ("momentum_engine", "momentum_paper", "scripts.run_momentum_paper_loop"),
    ],
)
def test_legacy_frontend_worker_names_start_canonical_paper_workers(
    tmp_path, monkeypatch, legacy_name, canonical_name, module
):
    monkeypatch.setattr(control, "RUNTIME_DIR", tmp_path)
    process = Mock(pid=4242)
    process.poll.return_value = None
    popen = Mock(return_value=process)
    monkeypatch.setattr(control.subprocess, "Popen", popen)

    result = control.WorkerControlService(startup_timeout=0).start(legacy_name)

    assert result == {
        "worker": canonical_name,
        "requested_worker_id": legacy_name,
        "canonical_worker_id": canonical_name,
        "deprecated_alias": True,
        "process_state": "running",
        "pid": 4242,
        "action": "started",
    }
    assert (tmp_path / f"{canonical_name}.pid").read_text() == "4242"
    assert not (tmp_path / f"{legacy_name}.pid").exists()
    popen.assert_called_once_with(
        [control.sys.executable, "-m", module],
        cwd=control.ROOT_DIR,
        stdout=ANY,
        stderr=ANY,
        start_new_session=True,
    )


def test_double_start_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setattr(control, "RUNTIME_DIR", tmp_path)
    process = Mock(pid=4242)
    process.poll.return_value = None
    popen = Mock(return_value=process)
    monkeypatch.setattr(control.subprocess, "Popen", popen)
    service = control.WorkerControlService(startup_timeout=0)
    monkeypatch.setattr(service, "_owns_pid", lambda name, pid: pid == 4242)
    first = service.start("scheduler")
    second = service.start("scheduler")
    assert first["action"] == "started"
    assert second == {
        "worker": "scheduler", "process_state": "running", "pid": 4242, "action": "noop",
        "requested_worker_id": "scheduler", "canonical_worker_id": "scheduler", "deprecated_alias": False,
    }
    popen.assert_called_once()


def test_immediate_exit_fails_start_and_removes_pid_file(tmp_path, monkeypatch):
    monkeypatch.setattr(control, "RUNTIME_DIR", tmp_path)
    log_dir = tmp_path / "canonical-logs"
    monkeypatch.setattr(control, "get_log_dir", lambda: log_dir)
    process = Mock(pid=4242)
    process.poll.return_value = 17
    monkeypatch.setattr(control.subprocess, "Popen", Mock(return_value=process))

    with pytest.raises(control.WorkerStartupError) as raised:
        control.WorkerControlService(startup_timeout=0.25).start("executor")

    assert not (tmp_path / "wyckoff_paper.pid").exists()
    message = str(raised.value)
    assert "Worker wyckoff_paper" in message
    assert "exit code 17" in message
    assert str((log_dir / "wyckoff_paper.log").resolve()) in message


def test_status_exposes_frontend_running_flag(tmp_path, monkeypatch):
    monkeypatch.setattr(control, "RUNTIME_DIR", tmp_path)
    service = control.WorkerControlService()
    (tmp_path / "scheduler.pid").write_text("4242")
    monkeypatch.setattr(service, "_owns_pid", lambda name, pid: name == "scheduler" and pid == 4242)

    statuses = service.status()

    assert statuses["scheduler"]["running"] is True
    assert statuses["scheduler"]["process_state"] == "running"
    assert statuses["scheduler"]["pid"] == 4242
    assert statuses["pipeline"]["running"] is False
    assert statuses["pipeline"]["process_state"] == "stopped"


def test_systemd_active_worker_does_not_require_runtime_pid_file(tmp_path, monkeypatch):
    monkeypatch.setattr(control, "RUNTIME_DIR", tmp_path)
    service = control.WorkerControlService(supervisor="systemd")
    systemctl = Mock(return_value=Mock(
        returncode=0, stdout="ActiveState=active\nMainPID=4242\n"
    ))
    monkeypatch.setattr(control.subprocess, "run", systemctl)
    monkeypatch.setattr(service, "_owns_pid", lambda name, pid: name == "scheduler" and pid == 4242)

    status = service.status()["scheduler"]

    assert not (tmp_path / "scheduler.pid").exists()
    assert status["running"] is True
    assert status["pid"] == 4242
    assert status["supervisor_state"] == "active"
    systemctl.assert_any_call(
        ["systemctl", "show", "signalmaker-scheduler.service", "--property=ActiveState", "--property=MainPID"],
        check=False, capture_output=True, text=True,
    )


def test_stopped_systemd_service_is_reported_stopped(tmp_path, monkeypatch):
    monkeypatch.setattr(control, "RUNTIME_DIR", tmp_path)
    service = control.WorkerControlService(supervisor="systemd")
    monkeypatch.setattr(control.subprocess, "run", Mock(return_value=Mock(
        returncode=0, stdout="ActiveState=inactive\nMainPID=0\n"
    )))

    status = service.status()["scheduler"]

    assert status["running"] is False
    assert status["pid"] is None
    assert status["process_state"] == "stopped"
    assert status["supervisor_state"] == "inactive"


def test_stale_local_pid_file_is_ignored_and_removed_on_stop(tmp_path, monkeypatch):
    monkeypatch.setattr(control, "RUNTIME_DIR", tmp_path)
    pid_file = tmp_path / "scheduler.pid"
    pid_file.write_text("4242")
    service = control.WorkerControlService(supervisor="local")
    monkeypatch.setattr(service, "_owns_pid", lambda _name, _pid: False)

    assert service.status()["scheduler"]["running"] is False
    assert service.stop("scheduler")["action"] == "noop"
    assert not pid_file.exists()


def test_stopping_current_job_requeues_claim_cleanly():
    class DB:
        def commit(self): pass
        def rollback(self): pass

    class Repo:
        db = DB()
        updates = []
        async def claim_next_analysis_job(self, *_args, **_kwargs):
            return {"id": 7, "attempts": 1, "payload": {"engine": "momentum"}}
        async def heartbeat_job(self, *_args): return True
        async def update_job_request(self, job_id, status, *, result=None):
            self.updates.append((job_id, status, result))

    class Service:
        async def run(self, **_kwargs):
            await asyncio.Event().wait()

    async def scenario():
        repo = Repo()
        task = asyncio.create_task(MarketAnalysisJobConsumer(repo, service_factory=lambda *_a, **_k: Service()).consume_one())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert repo.updates[-1][0:2] == (7, "queued")
        assert repo.updates[-1][2]["last_error"] == "worker stopped gracefully"

    asyncio.run(scenario())

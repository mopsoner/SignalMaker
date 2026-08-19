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
    popen = Mock(return_value=process)
    monkeypatch.setattr(control.subprocess, "Popen", popen)

    result = control.WorkerControlService().start(legacy_name)

    assert result == {
        "worker": canonical_name,
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
    popen = Mock(return_value=process)
    monkeypatch.setattr(control.subprocess, "Popen", popen)
    service = control.WorkerControlService()
    monkeypatch.setattr(service, "_owns_pid", lambda name, pid: pid == 4242)
    first = service.start("scheduler")
    second = service.start("scheduler")
    assert first["action"] == "started"
    assert second == {"worker": "scheduler", "process_state": "running", "pid": 4242, "action": "noop"}
    popen.assert_called_once()


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

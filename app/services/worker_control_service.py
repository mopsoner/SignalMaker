from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text

from app.core.logging import get_log_dir

ROOT_DIR = Path(__file__).resolve().parents[2]
RUNTIME_DIR = ROOT_DIR / ".runtime"
RUNTIME_DIR.mkdir(exist_ok=True)

# Public, stable identifiers.  In particular none of these aliases resolve by
# prefix, so an operation on a stock/ETF worker can never hit a crypto process.
WORKERS = {
    "pipeline": {"module": "scripts.run_pipeline_loop", "systemd_unit": "signalmaker-pipeline.service"},
    "wyckoff_paper": {"module": "scripts.run_wyckoff_paper_loop", "systemd_unit": "signalmaker-wyckoff-paper.service"},
    "kraken_candle_feed": {"module": "scripts.run_kraken_candle_feed_loop", "systemd_unit": "signalmaker-kraken-candle-feed.service"},
    "momentum_paper": {"module": "scripts.run_momentum_paper_loop", "systemd_unit": "signalmaker-momentum-paper.service"},
    "momentum_live": {"module": "scripts.run_momentum_live_loop", "systemd_unit": "signalmaker-momentum-live.service"},
    "wyckoff_live": {"module": "scripts.run_wyckoff_live_loop", "systemd_unit": "signalmaker-wyckoff-live.service"},
    "ibkr_ingestion": {"module": "scripts.run_ibkr_ingestion_loop", "systemd_unit": "signalmaker-ibkr-ingestion.service"},
    "stock_etf_analysis": {"module": "scripts.run_market_analysis_worker", "systemd_unit": "signalmaker-market-analysis.service"},
    "scheduler": {"module": "scripts.run_scheduler_loop", "systemd_unit": "signalmaker-scheduler.service"},
}


class WorkerStartupError(RuntimeError):
    """Raised when a managed worker exits during its startup check."""


# Worker names used by frontend bundles deployed before the paper/live split.
# Keep these aliases outside WORKERS so status responses and current clients only
# advertise canonical worker IDs, while a cached browser can still start/stop the
# equivalent paper worker during a rolling deployment.
LEGACY_WORKER_ALIASES = {
    "executor": "wyckoff_paper",
    "momentum_engine": "momentum_paper",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class WorkerControlService:
    WORKERS = WORKERS

    def __init__(self, db=None, *, stop_timeout: float = 10.0, startup_timeout: float = 0.25,
                 supervisor: str | None = None):
        self.db = db
        self.stop_timeout = stop_timeout
        self.startup_timeout = startup_timeout
        self.supervisor = supervisor or os.getenv("SIGNALMAKER_WORKER_SUPERVISOR", "local")
        if self.supervisor not in {"local", "systemd"}:
            raise ValueError("SIGNALMAKER_WORKER_SUPERVISOR must be 'local' or 'systemd'")

    def _paths(self, name: str) -> tuple[Path, Path, Path, Path]:
        return tuple(RUNTIME_DIR / f"{name}.{suffix}" for suffix in ("pid", "log", "state.json", "heartbeat.json"))

    def _definition(self, name: str) -> dict:
        if name not in WORKERS:  # deliberately exact; never fuzzy/prefix matching
            raise ValueError(f"Unknown worker: {name}")
        return WORKERS[name]

    @staticmethod
    def _canonical_name(name: str) -> str:
        return LEGACY_WORKER_ALIASES.get(name, name)

    def _read_pid(self, name: str) -> int | None:
        try:
            return int(self._paths(name)[0].read_text().strip())
        except (OSError, ValueError):
            return None

    def _owns_pid(self, name: str, pid: int | None) -> bool:
        if not pid:
            return False
        try:
            os.kill(pid, 0)
            arguments = [part.decode() for part in Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0") if part]
        except (OSError, UnicodeError):
            return False
        module = self._definition(name)["module"]
        return any(argument == "-m" and index + 1 < len(arguments) and arguments[index + 1] == module
                   for index, argument in enumerate(arguments))

    @staticmethod
    def _json(path: Path) -> dict:
        try:
            return json.loads(path.read_text())
        except (OSError, ValueError):
            return {}

    def _queue_status(self, name: str) -> dict:
        if self.db is None or name != "stock_etf_analysis":
            return {"state": "not_applicable" if name != "stock_etf_analysis" else "unknown"}
        rows = self.db.execute(text("""
            SELECT lower(status) status, count(*) count, max(heartbeat_at) heartbeat_at,
                   max(last_error) last_error FROM market_data_job_requests
            WHERE job_type='analysis' GROUP BY lower(status)
        """)).mappings().all()
        counts = {row["status"]: row["count"] for row in rows}
        running = next((row for row in rows if row["status"] == "running"), None)
        return {"state": "degraded" if counts.get("failed") else ("busy" if counts.get("running") else "ready"),
                "counts": counts, "job_heartbeat_at": str(running["heartbeat_at"]) if running else None,
                "last_error": next((row["last_error"] for row in rows if row["last_error"]), None)}

    def _systemd_state(self, name: str) -> tuple[str, int | None]:
        unit = self._definition(name)["systemd_unit"]
        completed = subprocess.run(
            ["systemctl", "show", unit, "--property=ActiveState", "--property=MainPID"],
            check=False, capture_output=True, text=True,
        )
        if completed.returncode != 0:
            return "unknown", None
        values = dict(line.split("=", 1) for line in completed.stdout.splitlines() if "=" in line)
        try:
            pid = int(values.get("MainPID", "0")) or None
        except ValueError:
            pid = None
        return values.get("ActiveState", "unknown"), pid

    def _process_state(self, name: str) -> tuple[str, int | None, bool]:
        if self.supervisor == "systemd":
            state, pid = self._systemd_state(name)
            owned = state == "active" and self._owns_pid(name, pid)
            return state, pid if owned else None, owned
        pid = self._read_pid(name)
        owned = self._owns_pid(name, pid)
        return "running" if owned else "stopped", pid if owned else None, owned

    def status(self) -> dict:
        result = {}
        for name, definition in WORKERS.items():
            supervisor_state, pid, owned = self._process_state(name)
            _, _, state_path, heartbeat_path = self._paths(name)
            state, heartbeat = self._json(state_path), self._json(heartbeat_path)
            result[name] = {
                "worker_id": name, "command": [sys.executable, "-m", definition["module"]],
                "pid": pid if owned else None, "running": owned,
                "process_state": "running" if owned else "stopped",
                "supervisor": self.supervisor, "supervisor_state": supervisor_state,
                "heartbeat_at": heartbeat.get("at"), "started_at": state.get("started_at"),
                "last_stopped_at": state.get("last_stopped_at"), "queue": self._queue_status(name),
            }
        return result

    def start(self, name: str) -> dict:
        name = self._canonical_name(name)
        definition = self._definition(name)
        if self.supervisor == "systemd":
            state, pid, owned = self._process_state(name)
            if owned:
                return {"worker": name, "process_state": "running", "pid": pid, "action": "noop"}
            subprocess.run(["systemctl", "start", definition["systemd_unit"]], check=True)
            state, pid, owned = self._process_state(name)
            if not owned:
                raise WorkerStartupError(f"Worker {name} systemd unit did not become active with its expected module")
            return {"worker": name, "process_state": "running", "pid": pid, "action": "started"}
        pid_file, _, state_file, _ = self._paths(name)
        log_dir = get_log_dir()
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{name}.log"
        pid = self._read_pid(name)
        if self._owns_pid(name, pid):
            return {"worker": name, "process_state": "running", "pid": pid, "action": "noop"}
        pid_file.unlink(missing_ok=True)
        log_handle = open(log_file, "ab")
        process = subprocess.Popen([sys.executable, "-m", definition["module"]], cwd=ROOT_DIR,
                                   stdout=log_handle, stderr=log_handle, start_new_session=True)
        log_handle.close()
        pid_file.write_text(str(process.pid))

        deadline = time.monotonic() + self.startup_timeout
        while True:
            exit_code = process.poll()
            if exit_code is not None:
                pid_file.unlink(missing_ok=True)
                raise WorkerStartupError(
                    f"Worker {name} exited during startup with exit code {exit_code}. "
                    f"Inspect the canonical log at {log_file.resolve()}."
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(0.05, remaining))

        state = self._json(state_file)
        state.update(started_at=_utc_now())
        state_file.write_text(json.dumps(state))
        return {"worker": name, "process_state": "running", "pid": process.pid, "action": "started"}

    def stop(self, name: str) -> dict:
        name = self._canonical_name(name)
        definition = self._definition(name)
        if self.supervisor == "systemd":
            _state, pid, owned = self._process_state(name)
            if not owned:
                return {"worker": name, "process_state": "stopped", "pid": None, "action": "noop"}
            subprocess.run(["systemctl", "stop", definition["systemd_unit"]], check=True)
            return {"worker": name, "process_state": "stopped", "pid": None, "action": "stopped"}
        pid_file, _, state_file, _ = self._paths(name)
        pid = self._read_pid(name)
        if not self._owns_pid(name, pid):
            pid_file.unlink(missing_ok=True)
            return {"worker": name, "process_state": "stopped", "pid": None, "action": "noop"}
        os.kill(pid, signal.SIGTERM)
        deadline = time.monotonic() + self.stop_timeout
        while time.monotonic() < deadline and self._owns_pid(name, pid):
            time.sleep(0.05)
        if self._owns_pid(name, pid):
            os.kill(pid, signal.SIGKILL)
        pid_file.unlink(missing_ok=True)
        state = self._json(state_file)
        state.update(last_stopped_at=_utc_now())
        state_file.write_text(json.dumps(state))
        return {"worker": name, "process_state": "stopped", "pid": None, "action": "stopped"}

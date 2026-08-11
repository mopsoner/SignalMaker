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
    "pipeline": {"module": "scripts.run_pipeline_loop"},
    "executor": {"module": "scripts.run_executor_loop"},
    "kraken_candle_feed": {"module": "scripts.run_kraken_candle_feed_loop"},
    "momentum_engine": {"module": "scripts.run_momentum_engine_loop"},
    "momentum_backtest": {"module": "scripts.run_momentum_backtest_worker"},
    "ibkr_ingestion": {"module": "scripts.run_ibkr_ingestion_loop"},
    "stock_etf_analysis": {"module": "scripts.run_market_analysis_worker"},
    "scheduler": {"module": "scripts.run_scheduler_loop"},
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class WorkerControlService:
    WORKERS = WORKERS

    def __init__(self, db=None, *, stop_timeout: float = 10.0):
        self.db = db
        self.stop_timeout = stop_timeout

    def _paths(self, name: str) -> tuple[Path, Path, Path, Path]:
        return tuple(RUNTIME_DIR / f"{name}.{suffix}" for suffix in ("pid", "log", "state.json", "heartbeat.json"))

    def _definition(self, name: str) -> dict:
        if name not in WORKERS:  # deliberately exact; never fuzzy/prefix matching
            raise ValueError(f"Unknown worker: {name}")
        return WORKERS[name]

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
            command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode()
        except (OSError, UnicodeError):
            return False
        return self._definition(name)["module"] in command

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

    def status(self) -> dict:
        result = {}
        for name, definition in WORKERS.items():
            pid = self._read_pid(name)
            owned = self._owns_pid(name, pid)
            _, _, state_path, heartbeat_path = self._paths(name)
            state, heartbeat = self._json(state_path), self._json(heartbeat_path)
            result[name] = {
                "worker_id": name, "command": [sys.executable, "-m", definition["module"]],
                "pid": pid if owned else None, "running": owned,
                "process_state": "running" if owned else "stopped",
                "heartbeat_at": heartbeat.get("at"), "started_at": state.get("started_at"),
                "last_stopped_at": state.get("last_stopped_at"), "queue": self._queue_status(name),
            }
        return result

    def start(self, name: str) -> dict:
        definition = self._definition(name)
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
        state = self._json(state_file)
        state.update(started_at=_utc_now())
        state_file.write_text(json.dumps(state))
        return {"worker": name, "process_state": "running", "pid": process.pid, "action": "started"}

    def stop(self, name: str) -> dict:
        self._definition(name)
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

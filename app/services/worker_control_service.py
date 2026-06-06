from __future__ import annotations

import os
import signal
import subprocess
import sys
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parents[2] / ".runtime"
RUNTIME_DIR.mkdir(exist_ok=True)
ROOT_DIR = Path(__file__).resolve().parents[2]

WORKERS = {
    "pipeline": {"module": "scripts.run_pipeline_loop", "pid_file": RUNTIME_DIR / "pipeline.pid", "log_file": RUNTIME_DIR / "pipeline.log"},
    "executor": {"module": "scripts.run_executor_loop", "pid_file": RUNTIME_DIR / "executor.pid", "log_file": RUNTIME_DIR / "executor.log"},
    "scheduler": {"module": "scripts.run_scheduler_loop", "pid_file": RUNTIME_DIR / "scheduler.pid", "log_file": RUNTIME_DIR / "scheduler.log"},
}


class WorkerControlService:
    def _read_pid(self, name: str) -> int | None:
        pid_file = WORKERS[name]["pid_file"]
        if not pid_file.exists():
            return None
        try:
            return int(pid_file.read_text().strip())
        except Exception:
            return None

    def _is_running(self, pid: int | None) -> bool:
        if not pid:
            return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def status(self) -> dict:
        out = {}
        for name in WORKERS:
            pid = self._read_pid(name)
            out[name] = {"running": self._is_running(pid), "pid": pid}
        return out

    def start(self, name: str) -> dict:
        if name not in WORKERS:
            raise ValueError(f"Unknown worker: {name}")
        pid = self._read_pid(name)
        if self._is_running(pid):
            return {"worker": name, "running": True, "pid": pid, "action": "noop"}
        log_handle = open(WORKERS[name]["log_file"], "ab")
        process = subprocess.Popen(
            [sys.executable, "-m", WORKERS[name]["module"]],
            cwd=str(ROOT_DIR),
            stdout=log_handle,
            stderr=log_handle,
            start_new_session=True,
        )
        WORKERS[name]["pid_file"].write_text(str(process.pid))
        return {"worker": name, "running": True, "pid": process.pid, "action": "started"}

    def stop(self, name: str) -> dict:
        if name not in WORKERS:
            raise ValueError(f"Unknown worker: {name}")
        pid = self._read_pid(name)
        if not self._is_running(pid):
            return {"worker": name, "running": False, "pid": pid, "action": "noop"}
        os.kill(pid, signal.SIGTERM)
        return {"worker": name, "running": False, "pid": pid, "action": "stopped"}

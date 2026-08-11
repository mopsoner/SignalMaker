"""Shared configuration for worker log destinations."""

import os
from pathlib import Path

DEFAULT_LOG_DIR = Path(__file__).resolve().parents[2] / "logs"


def get_log_dir() -> Path:
    """Return the explicitly configured directory containing worker logs."""
    return Path(os.environ.get("SIGNALMAKER_LOG_DIR", DEFAULT_LOG_DIR)).expanduser()


def worker_log_candidates(worker_name: str) -> tuple[Path, ...]:
    """Return current and legacy log paths, in no particular priority order."""
    log_dir = get_log_dir()
    return (log_dir / f"{worker_name}.log", log_dir.parent / ".runtime" / f"{worker_name}.log")

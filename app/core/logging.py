"""Shared configuration for worker log destinations."""

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

DEFAULT_LOG_DIR = Path(__file__).resolve().parents[2] / "logs"


def get_log_dir() -> Path:
    """Return the explicitly configured directory containing worker logs."""
    return Path(os.environ.get("SIGNALMAKER_LOG_DIR", DEFAULT_LOG_DIR)).expanduser()


def worker_log_candidates(worker_name: str) -> tuple[Path, ...]:
    """Return current and legacy log paths, in no particular priority order."""
    log_dir = get_log_dir()
    return (log_dir / f"{worker_name}.log", log_dir.parent / ".runtime" / f"{worker_name}.log")


ERROR_LOGGER_NAME = "signalmaker.errors"


def configure_error_logging() -> logging.Logger:
    """Configure the persistent application error log and return its logger."""
    logger = logging.getLogger(ERROR_LOGGER_NAME)
    log_path = get_log_dir() / "application.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Lifespan can be started repeatedly by tests and reloaders. Avoid adding a
    # second handler for the same destination on every startup.
    resolved_path = log_path.resolve()
    for handler in logger.handlers:
        if isinstance(handler, RotatingFileHandler) and Path(handler.baseFilename).resolve() == resolved_path:
            return logger

    handler = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.ERROR)
    logger.propagate = False
    return logger

import logging
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "logs"
LOG_FILE = LOG_DIR / "executor.log"


class RepeatFilter(logging.Filter):
    def __init__(self, seconds: int = 120) -> None:
        super().__init__()
        self.seconds = seconds
        self.last_seen: dict[str, float] = {}

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        # Always keep real trading actions and errors.
        keep_words = (
            "opened",
            "sold",
            "filled",
            "repaired",
            "failed",
            "error",
            "started",
            "startup",
        )
        if record.levelno >= logging.WARNING or any(word in message.lower() for word in keep_words):
            return True
        # Suppress repeated idle summaries.
        noisy_prefixes = (
            "executor summary=",
            "position sync summary=",
            "candle feed summary=",
            "candidates summary=",
        )
        if not message.startswith(noisy_prefixes):
            return True
        now = time.monotonic()
        previous = self.last_seen.get(message)
        if previous is not None and now - previous < self.seconds:
            return False
        self.last_seen[message] = now
        return True


_REPEAT_FILTER = RepeatFilter()


def setup_logging(name: str = "raspberry-executor") -> logging.Logger:
    LOG_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.addFilter(_REPEAT_FILTER)
    logger.addHandler(console)

    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(_REPEAT_FILTER)
    logger.addHandler(file_handler)

    return logger


def tail_logs(lines: int = 300) -> list[str]:
    if not LOG_FILE.exists():
        return []
    data = LOG_FILE.read_text(errors="replace").splitlines()
    return data[-lines:]

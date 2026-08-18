import logging
from pathlib import Path

from app.core.logging import configure_error_logging


def test_error_logger_writes_to_rotating_application_log(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("SIGNALMAKER_LOG_DIR", str(tmp_path))
    logger = configure_error_logging()

    logger.error("request_failed request_id=test-request method=GET path=/api/test status=500")
    for handler in logger.handlers:
        handler.flush()

    contents = (tmp_path / "application.log").read_text(encoding="utf-8")
    assert "ERROR request_failed request_id=test-request" in contents
    assert "path=/api/test status=500" in contents

    # Do not leave a handler open on pytest's temporary directory.
    for handler in list(logger.handlers):
        if Path(getattr(handler, "baseFilename", "")).parent == tmp_path:
            logger.removeHandler(handler)
            handler.close()
    logger.setLevel(logging.NOTSET)

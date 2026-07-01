import pytest
from pydantic import ValidationError

from app.core.config import Settings


def test_settings_accept_supported_signal_execution_interval():
    settings = Settings(SIGNAL_EXECUTION_INTERVAL="1h")

    assert settings.signal_execution_interval == "1h"
    assert settings.signal_config()["execution_interval"] == "1h"


def test_settings_reject_unsupported_signal_execution_interval():
    with pytest.raises(ValidationError, match="Unsupported signal execution interval"):
        Settings(SIGNAL_EXECUTION_INTERVAL="30m")

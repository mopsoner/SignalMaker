import importlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import raspberry_executor.tui_api as tui_api


def reload_tui_api(monkeypatch, **env: str):
    for key in ("SIGNALMAKER_BASE_URL", "EXECUTOR_API_PORT", "APP_PORT"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return importlib.reload(tui_api)


def test_tui_base_url_fallback_defaults_to_8080(monkeypatch) -> None:
    module = reload_tui_api(monkeypatch)

    assert module.default_base_url() == "http://127.0.0.1:8080"
    assert module.BASE_URL == "http://127.0.0.1:8080"


def test_tui_base_url_prefers_executor_api_port(monkeypatch) -> None:
    module = reload_tui_api(monkeypatch, EXECUTOR_API_PORT="9090", APP_PORT="7070")

    assert module.default_base_url() == "http://127.0.0.1:9090"
    assert module.BASE_URL == "http://127.0.0.1:9090"


def test_tui_base_url_uses_signal_env_override_and_strips_slash(monkeypatch) -> None:
    module = reload_tui_api(monkeypatch, SIGNALMAKER_BASE_URL="http://example.test:1234/")

    assert module.BASE_URL == "http://example.test:1234"

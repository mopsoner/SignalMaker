import os

from raspberry_executor import admin_settings_bridge


def test_admin_settings_bridge_ignores_masked_kraken_credentials(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {
                "kraken": {
                    "kraken_api_key": "********",
                    "kraken_secret_key": "********",
                    "kraken_base_url": "https://api.kraken.com",
                }
            }

    monkeypatch.delenv("KRAKEN_API_KEY", raising=False)
    monkeypatch.delenv("KRAKEN_SECRET_KEY", raising=False)
    monkeypatch.setattr(admin_settings_bridge, "_local_runtime_settings", lambda: ({}, None))
    monkeypatch.setattr(admin_settings_bridge.requests, "get", lambda *args, **kwargs: FakeResponse())

    result = admin_settings_bridge.apply_admin_settings_to_environ("https://signalmaker.test")

    assert result["kraken_api_key_in_admin_payload"] is True
    assert result["kraken_secret_key_in_admin_payload"] is True
    assert result["kraken_credentials_source"] == ".env"
    assert result["kraken_admin_credentials_ignored"] is True
    assert "KRAKEN_API_KEY" not in os.environ
    assert "KRAKEN_SECRET_KEY" not in os.environ


def test_admin_settings_bridge_ignores_unmasked_kraken_credentials(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {
                "kraken": {
                    "kraken_api_key": "key",
                    "kraken_secret_key": "secret",
                    "kraken_base_url": "https://api.kraken.com",
                }
            }

    monkeypatch.delenv("KRAKEN_API_KEY", raising=False)
    monkeypatch.delenv("KRAKEN_SECRET_KEY", raising=False)
    monkeypatch.setattr(admin_settings_bridge, "_local_runtime_settings", lambda: ({}, None))
    monkeypatch.setattr(admin_settings_bridge.requests, "get", lambda *args, **kwargs: FakeResponse())

    result = admin_settings_bridge.apply_admin_settings_to_environ("https://signalmaker.test")

    assert result["kraken_credentials_source"] == ".env"
    assert result["kraken_admin_credentials_ignored"] is True
    assert "KRAKEN_API_KEY" not in os.environ
    assert "KRAKEN_SECRET_KEY" not in os.environ


def test_admin_settings_bridge_runtime_db_credentials_do_not_override_env(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {}

    monkeypatch.setenv("KRAKEN_API_KEY", "env-key")
    monkeypatch.setenv("KRAKEN_SECRET_KEY", "env-secret")
    monkeypatch.setattr(
        admin_settings_bridge,
        "_local_runtime_settings",
        lambda: (
            {
                "kraken": {
                    "kraken_api_key": "runtime-key",
                    "kraken_secret_key": "runtime-secret",
                    "kraken_base_url": "https://runtime.kraken",
                }
            },
            None,
        ),
    )
    monkeypatch.setattr(admin_settings_bridge.requests, "get", lambda *args, **kwargs: FakeResponse())

    result = admin_settings_bridge.apply_admin_settings_to_environ("https://signalmaker.test")

    assert result["kraken_api_key_in_admin_payload"] is True
    assert result["kraken_secret_key_in_admin_payload"] is True
    assert result["kraken_credentials_source"] == ".env"
    assert result["kraken_admin_credentials_ignored"] is True
    assert os.environ["KRAKEN_API_KEY"] == "env-key"
    assert os.environ["KRAKEN_SECRET_KEY"] == "env-secret"

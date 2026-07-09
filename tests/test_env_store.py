from __future__ import annotations

from raspberry_executor import config, env_store


def test_default_order_quote_amount_is_20(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    monkeypatch.setattr(env_store, "ENV_PATH", env_path)
    monkeypatch.setattr(env_store, "EXAMPLE_PATH", tmp_path / ".env.example")

    env_store.write_env({})

    text = env_path.read_text()
    assert "ORDER_QUOTE_AMOUNT=20" in text
    assert env_store.read_env()["ORDER_QUOTE_AMOUNT"] == "20"


def test_config_fallback_order_quote_amount_is_50(monkeypatch):
    monkeypatch.setattr(config, "read_env", lambda: {})
    monkeypatch.setattr(config, "_runtime_overrides", lambda: {})

    settings = config.load_settings()

    assert settings.order_quote_amount == 50.0


def test_legacy_momentum_rsi_bounds_are_ignored(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    monkeypatch.setattr(env_store, "ENV_PATH", env_path)
    monkeypatch.setattr(env_store, "EXAMPLE_PATH", tmp_path / ".env.example")

    env_store.write_env({"MOMENTUM_BUYABLE_RSI_1H_MIN": "42", "MOMENTUM_BUYABLE_RSI_1H_MAX": "61"})

    text = env_path.read_text()
    assert "MOMENTUM_BUYABLE_RSI_1H_MIN" not in text
    assert "MOMENTUM_BUYABLE_RSI_1H_MAX" not in text
    values = env_store.read_env()
    assert "MOMENTUM_BUYABLE_RSI_1H_MIN" not in values
    assert "MOMENTUM_BUYABLE_RSI_1H_MAX" not in values


def test_migrate_env_to_minimal_preserves_supported_and_maps_quote_alias(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    monkeypatch.setattr(env_store, "ENV_PATH", env_path)
    monkeypatch.setattr(env_store, "EXAMPLE_PATH", tmp_path / ".env.example")
    env_path.write_text("KRAKEN_API_KEY=abc\nCANDLE_FEED_QUOTES=eur, usd\nADMIN_PASSWORD=secret\nEXECUTOR_DASHBOARD_PORT=9999\n")

    assert env_store.migrate_env_to_minimal() is True

    text = env_path.read_text()
    assert "KRAKEN_API_KEY=abc" in text
    assert "QUOTE_ASSETS=EUR,USD" in text
    assert "ADMIN_PASSWORD" not in text
    assert "EXECUTOR_DASHBOARD_PORT" not in text
    assert "CANDLE_FEED_QUOTES" not in text
    assert env_store.read_env()["QUOTE_ASSETS"] == "EUR,USD"


def test_runtime_overrides_do_not_include_kraken_credentials(monkeypatch):
    import sys
    import types

    runtime_settings = types.ModuleType("app.services.runtime_settings")
    runtime_settings.load_runtime_settings = lambda: {
        "kraken": {
            "kraken_base_url": "https://runtime.kraken.test",
            "kraken_api_key": "runtime-key",
            "kraken_secret_key": "runtime-secret",
        },
        "executor": {"execution_exchange": "kraken_pro", "quote_assets": ["eur", "usd"]},
    }
    monkeypatch.setitem(sys.modules, "app.services.runtime_settings", runtime_settings)

    overrides = config._runtime_overrides()

    assert overrides["KRAKEN_BASE_URL"] == "https://runtime.kraken.test"
    assert overrides["EXECUTION_EXCHANGE"] == "kraken_pro"
    assert overrides["QUOTE_ASSETS"] == "eur,usd"
    assert "KRAKEN_API_KEY" not in overrides
    assert "KRAKEN_SECRET_KEY" not in overrides


def test_load_settings_keeps_kraken_credentials_from_read_env(monkeypatch):
    monkeypatch.setattr(
        config,
        "read_env",
        lambda: {
            "KRAKEN_API_KEY": "env-key",
            "KRAKEN_SECRET_KEY": "env-secret",
            "KRAKEN_BASE_URL": "https://env.kraken.test",
            "EXECUTION_EXCHANGE": "kraken",
            "QUOTE_ASSETS": "USD,USDC",
        },
    )
    monkeypatch.setattr(
        config,
        "_runtime_overrides",
        lambda: {
            "KRAKEN_API_KEY": "runtime-key",
            "KRAKEN_SECRET_KEY": "runtime-secret",
            "KRAKEN_BASE_URL": "https://runtime.kraken.test",
            "EXECUTION_EXCHANGE": "kraken_pro",
            "QUOTE_ASSETS": "EUR,USD",
        },
    )

    settings = config.load_settings()

    assert settings.kraken_api_key == "env-key"
    assert settings.kraken_secret_key == "env-secret"
    assert settings.kraken_base_url == "https://runtime.kraken.test"
    assert settings.exchange == "kraken_pro"
    assert settings.quote_assets == ["EUR", "USD"]

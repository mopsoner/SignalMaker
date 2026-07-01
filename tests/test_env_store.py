from __future__ import annotations

from raspberry_executor import env_store


def test_momentum_rsi_bounds_are_read_and_written(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    monkeypatch.setattr(env_store, "ENV_PATH", env_path)
    monkeypatch.setattr(env_store, "EXAMPLE_PATH", tmp_path / ".env.example")

    env_store.write_env({"MOMENTUM_BUYABLE_RSI_1H_MIN": "42", "MOMENTUM_BUYABLE_RSI_1H_MAX": "61"})

    text = env_path.read_text()
    assert "MOMENTUM_BUYABLE_RSI_1H_MIN=42" in text
    assert "MOMENTUM_BUYABLE_RSI_1H_MAX=61" in text
    values = env_store.read_env()
    assert values["MOMENTUM_BUYABLE_RSI_1H_MIN"] == "42"
    assert values["MOMENTUM_BUYABLE_RSI_1H_MAX"] == "61"


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

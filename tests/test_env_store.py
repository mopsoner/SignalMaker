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

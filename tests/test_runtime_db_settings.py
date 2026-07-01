from raspberry_executor import runtime_db_settings


def test_lightweight_runtime_settings_bootstraps_from_env(monkeypatch, tmp_path):
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "EXECUTION_EXCHANGE=coinbase",
                "KRAKEN_QUOTE_ASSETS=EUR,USD",
                "KRAKEN_REST_BASE=https://rest.kraken.test",
                "KRAKEN_API_KEY=api-key",
                "KRAKEN_SECRET_KEY=secret-key",
            ]
        )
    )
    monkeypatch.setattr(runtime_db_settings, "ENV_PATH", env_path)
    monkeypatch.setattr(runtime_db_settings, "_db_rows", lambda: ([], None))
    for key in runtime_db_settings.LIGHTWEIGHT_BOOTSTRAP_ENV_ALIASES:
        monkeypatch.delenv(key, raising=False)

    payload, meta = runtime_db_settings.load_runtime_settings_lightweight()

    assert meta["db_error"] is None
    assert payload["executor"]["execution_exchange"] == "coinbase"
    assert payload["executor"]["quote_assets"] == "EUR,USD"
    assert payload["kraken"]["kraken_base_url"] == "https://rest.kraken.test"
    assert payload["kraken"]["kraken_api_key"] == "api-key"
    assert payload["kraken"]["kraken_secret_key"] == "secret-key"


def test_lightweight_runtime_settings_prefers_db_rows_over_env(monkeypatch, tmp_path):
    env_path = tmp_path / ".env"
    env_path.write_text("EXECUTION_EXCHANGE=env-exchange\nQUOTE_ASSETS=EUR\n")
    monkeypatch.setattr(runtime_db_settings, "ENV_PATH", env_path)
    monkeypatch.setattr(
        runtime_db_settings,
        "_db_rows",
        lambda: (
            [
                ("executor", "execution_exchange", "db-exchange"),
                ("executor", "quote_assets", "USD"),
            ],
            None,
        ),
    )
    for key in runtime_db_settings.LIGHTWEIGHT_BOOTSTRAP_ENV_ALIASES:
        monkeypatch.delenv(key, raising=False)

    payload, _ = runtime_db_settings.load_runtime_settings_lightweight()

    assert payload["executor"]["execution_exchange"] == "db-exchange"
    assert payload["executor"]["quote_assets"] == "USD"

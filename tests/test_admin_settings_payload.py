from app.services import runtime_settings


def test_load_admin_settings_omits_display_alias_duplicates(monkeypatch):
    def fake_runtime_settings(db=None):
        return {
            "general": {"admin_token": "token"},
            "binance": {"binance_api_key": "binance-key", "binance_secret_key": "binance-secret"},
            "kraken": {
                "execution_exchange": "binance",
                "kraken_base_url": "https://api.kraken.com",
                "kraken_api_key": "kraken-key",
                "kraken_secret_key": "kraken-secret",
            },
        }

    monkeypatch.setattr(runtime_settings, "load_runtime_settings", fake_runtime_settings)

    payload = runtime_settings.load_admin_settings()

    assert payload["kraken"] == {
        "execution_exchange": "binance",
        "kraken_base_url": "https://api.kraken.com",
        "kraken_api_key": "kraken-key",
        "kraken_secret_key": "kraken-secret",
    }
    assert "EXECUTION_EXCHANGE" not in payload["kraken"]
    assert "KRAKEN_BASE_URL" not in payload["kraken"]
    assert "BINANCE_API_KEY" not in payload["binance"]

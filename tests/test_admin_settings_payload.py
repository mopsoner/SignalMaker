from app.services import runtime_settings


def test_load_admin_settings_omits_display_alias_duplicates(monkeypatch):
    def fake_runtime_settings(db=None):
        return {
            "general": {"admin_token": "token"},
            "executor": {"execution_exchange": "binance", "quote_assets": "USDC"},
            "binance": {"binance_api_key": "binance-key", "binance_secret_key": "binance-secret"},
            "kraken": {
                "kraken_base_url": "https://api.kraken.com",
                "kraken_api_key": "kraken-key",
                "kraken_secret_key": "kraken-secret",
            },
        }

    monkeypatch.setattr(runtime_settings, "load_runtime_settings", fake_runtime_settings)

    payload = runtime_settings.load_admin_settings()

    assert payload["executor"] == {"execution_exchange": "binance", "quote_assets": "USDC"}
    assert payload["kraken"] == {
        "kraken_exchange_name": "kraken",
        "kraken_base_url": "https://api.kraken.com",
        "kraken_api_key": "kraken-key",
        "kraken_secret_key": "kraken-secret",
    }
    assert "EXECUTION_EXCHANGE" not in payload["kraken"]
    assert "KRAKEN_BASE_URL" not in payload["kraken"]
    assert "BINANCE_API_KEY" not in payload["binance"]


def test_load_runtime_settings_canonicalizes_stored_alias_rows(monkeypatch):
    class FakeRow:
        def __init__(self, category, key, value):
            self.category = category
            self.key = key
            self.value = value

    class FakeScalars:
        def all(self):
            return [
                FakeRow("kraken", "EXECUTION_EXCHANGE", "kraken"),
                FakeRow("kraken", "KRAKEN_BASE_URL", "https://kraken.test"),
                FakeRow("binance", "BINANCE_API_KEY", "binance-alias-key"),
                FakeRow("admin/security", "ADMIN_TOKEN", "admin-alias-token"),
            ]

    class FakeResult:
        def scalars(self):
            return FakeScalars()

    class FakeDb:
        def execute(self, statement):
            return FakeResult()

    payload = runtime_settings.load_runtime_settings(FakeDb())

    assert payload["executor"]["execution_exchange"] == "kraken"
    assert payload["kraken"]["kraken_base_url"] == "https://kraken.test"
    assert payload["binance"]["binance_api_key"] == "binance-alias-key"
    assert payload["general"]["admin_token"] == "admin-alias-token"
    assert "EXECUTION_EXCHANGE" not in payload["kraken"]
    assert "KRAKEN_BASE_URL" not in payload["kraken"]
    assert "BINANCE_API_KEY" not in payload["binance"]


def test_normalize_admin_payload_uppercase_kraken_keys():
    payload = runtime_settings._normalize_admin_payload(
        {"kraken": {"KRAKEN_API_KEY": "key", "KRAKEN_SECRET_KEY": "secret", "KRAKEN_BASE_URL": "https://k"}}
    )

    assert payload["kraken"]["kraken_api_key"] == "key"
    assert payload["kraken"]["kraken_secret_key"] == "secret"
    assert payload["kraken"]["kraken_base_url"] == "https://k"
    assert "KRAKEN_API_KEY" not in payload["kraken"]


def test_legacy_alias_rows_do_not_override_canonical_values():
    class FakeRow:
        def __init__(self, category, key, value):
            self.category = category
            self.key = key
            self.value = value

    class FakeScalars:
        def all(self):
            return [
                FakeRow("kraken", "kraken_api_key", "real-api-key"),
                FakeRow("kraken", "kraken_secret_key", "real-secret-key"),
                FakeRow("kraken", "KRAKEN_API_KEY", "bad"),
                FakeRow("kraken", "KRAKEN_SECRET_KEY", "bad"),
            ]

    class FakeResult:
        def scalars(self):
            return FakeScalars()

    class FakeDb:
        def execute(self, statement):
            return FakeResult()

    payload = runtime_settings.load_runtime_settings(FakeDb())

    assert payload["kraken"]["kraken_api_key"] == "real-api-key"
    assert payload["kraken"]["kraken_secret_key"] == "real-secret-key"


def test_load_admin_settings_returns_curated_sections_with_empty_defaults(monkeypatch):
    def fake_runtime_settings(db=None):
        return {
            "general": {"app_name": "SignalMaker", "admin_token": "secret"},
            "executor": {"execution_exchange": "kraken", "quote_assets": "USDC"},
            "binance": {"binance_rest_base": "https://binance.test", "binance_quote_assets": "USDT"},
            "kraken": {"kraken_base_url": "https://api.kraken.com", "kraken_api_key": "key", "kraken_secret_key": None},
            "market_data": {"binance_max_symbols": 25},
            "strategy": {"planner_min_score": 4},
        }

    monkeypatch.setattr(runtime_settings, "load_runtime_settings", fake_runtime_settings)

    payload = runtime_settings.load_admin_settings()

    assert "market_data" not in payload
    assert "strategy" not in payload
    assert "admin_token" not in payload["general"]
    assert "binance_quote_assets" not in payload["binance"]
    assert payload["executor"]["quote_assets"] == "USDC"
    assert payload["kraken"]["kraken_api_key"] == "key"
    assert payload["kraken"]["kraken_secret_key"] == ""

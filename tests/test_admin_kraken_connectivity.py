from app.api.routes import admin_settings


def test_kraken_admin_test_requires_credentials(monkeypatch):
    monkeypatch.setattr(
        admin_settings,
        "load_runtime_settings",
        lambda db: {"kraken": {"kraken_base_url": "https://api.kraken.com", "kraken_api_key": "", "kraken_secret_key": ""}},
    )

    payload = admin_settings.test_kraken(db=None)

    assert payload["status"] == "error"
    assert payload["error"] == "missing_kraken_api_credentials"
    assert payload["api_key_loaded"] is False
    assert payload["secret_key_loaded"] is False


def test_kraken_admin_test_uses_private_account_credentials(monkeypatch):
    calls = []

    class FakeKrakenClient:
        def __init__(self, base_url, api_key, secret_key, dry_run=True):
            self.base_url = base_url
            self.api_key = api_key
            self.secret_key = secret_key
            self.dry_run = dry_run
            calls.append((base_url, api_key, secret_key, dry_run))

        def is_configured(self):
            return bool(self.api_key and self.secret_key)

        def account(self):
            calls.append("account")
            return {"ZUSD": "1.23", "XXBT": "0.01"}

    monkeypatch.setattr(admin_settings, "KrakenClient", FakeKrakenClient)
    monkeypatch.setattr(
        admin_settings,
        "load_runtime_settings",
        lambda db: {
            "kraken": {
                "kraken_base_url": "https://api.kraken.com/",
                "kraken_api_key": "kraken-key",
                "kraken_secret_key": "kraken-secret",
            }
        },
    )

    payload = admin_settings.test_kraken(db=None)

    assert calls == [("https://api.kraken.com", "kraken-key", "kraken-secret", True), "account"]
    assert payload["status"] == "ok"
    assert payload["api_key_loaded"] is True
    assert payload["secret_key_loaded"] is True
    assert payload["account_keys"] == ["XXBT", "ZUSD"]

from types import SimpleNamespace

from raspberry_executor import kraken_full_smoke_test
from raspberry_executor.config import Settings


def _settings(**overrides):
    base = dict(
        signalmaker_base_url="https://signalmaker.test",
        gateway_id="raspberry-fr-1",
        poll_seconds=15,
        dry_run=True,
        quote_assets=["USDC"],
        allowed_symbols=["USDC"],
        order_quote_amount=20.0,
        max_candidate_age_seconds=900,
        kraken_base_url="https://api.kraken.com",
        kraken_api_key="",
        kraken_secret_key="",
        exchange="kraken",
        ibkr_market_feed_enabled=False,
        ibkr_cp_base_url="https://localhost:5000/v1/api",
        ibkr_cp_verify_ssl=False,
        ibkr_cp_timeout_seconds=30,
        ibkr_market_feed_poll_seconds=3600,
        ibkr_market_feed_intervals=["1d"],
        ibkr_market_feed_period="2y",
        ibkr_market_feed_bar="1d",
        ibkr_market_feed_source="Last",
        ibkr_market_feed_outside_rth=False,
        ibkr_market_feed_max_workers=1,
        ibkr_market_feed_requests_per_minute=20,
        ibkr_market_feed_limit=300,
        ibkr_market_feed_queue_analysis=False,
        ibkr_market_feed_universes=[],
        ibkr_market_feed_asset_types=[],
        ibkr_market_feed_symbols=[],
        ibkr_contract_cache_path="cache.json",
        ibkr_market_feed_retry_queue_path="retry.json",
        signalmaker_stock_etf_ibkr_ingest_path="/api/v1/stocks-etfs/ibkr/candles",
        signalmaker_stock_etf_assets_path="/api/v1/stocks-etfs/assets",
    )
    base.update(overrides)
    return Settings(**base)


def test_runtime_overrides_apply_admin_kraken_credentials(monkeypatch):
    monkeypatch.setenv("KRAKEN_API_KEY", "admin-key")
    monkeypatch.setenv("KRAKEN_SECRET_KEY", "admin-secret")
    monkeypatch.setenv("KRAKEN_BASE_URL", "https://kraken.test/")
    monkeypatch.setenv("QUOTE_ASSETS", "usdc, eur")

    settings = kraken_full_smoke_test._settings_with_runtime_overrides(_settings())

    assert settings.kraken_api_key == "admin-key"
    assert settings.kraken_secret_key == "admin-secret"
    assert settings.kraken_base_url == "https://kraken.test"
    assert settings.quote_assets == ["USDC", "EUR"]


def test_run_smoke_uses_admin_bridge_credentials_for_private_checks(monkeypatch):
    file_settings = _settings()
    monkeypatch.setattr(kraken_full_smoke_test, "ensure_env", lambda: None)
    monkeypatch.setattr(kraken_full_smoke_test, "load_settings", lambda: file_settings)

    def fake_bridge(base_url):
        assert base_url == "https://signalmaker.test"
        monkeypatch.setenv("KRAKEN_API_KEY", "admin-key")
        monkeypatch.setenv("KRAKEN_SECRET_KEY", "admin-secret")
        return {"applied": True, "kraken_base_url": "https://api.kraken.com"}

    monkeypatch.setattr(kraken_full_smoke_test, "apply_admin_settings_to_environ", fake_bridge)
    monkeypatch.setattr(
        kraken_full_smoke_test,
        "_fetch_admin_kraken_credential_status",
        lambda base_url: {"checked": True, "api_key_loaded": True, "secret_key_loaded": True, "status": "ok"},
    )
    monkeypatch.setattr(kraken_full_smoke_test, "_discover_default_symbol", lambda base_url, quote_assets: "BTCUSDC")
    monkeypatch.setattr(kraken_full_smoke_test, "fetch_kraken_ohlc", lambda *args, **kwargs: [1, 2, 3])
    monkeypatch.setattr(kraken_full_smoke_test, "discover_kraken_spot_symbols", lambda *args, **kwargs: ["BTCUSDC"])
    monkeypatch.setattr(kraken_full_smoke_test, "discover_kraken_margin_symbols", lambda *args, **kwargs: ["BTCUSDC"])

    class FakeClient:
        def __init__(self, base_url, api_key, secret_key, dry_run=True):
            self.base_url = base_url
            self.api_key = api_key
            self.secret_key = secret_key
            self.dry_run = dry_run

        def is_configured(self):
            return bool(self.api_key and self.secret_key)

        def _public(self, path):
            return {"unixtime": 1}

        def _pair_info(self, symbol):
            return {"altname": symbol}

        def current_price(self, symbol):
            return 100.0

        def place_market_entry(self, *args):
            return {"orderId": "entry"}

        def place_exit_limit(self, *args):
            return {"orderId": "tp"}

        def place_stop_loss(self, *args):
            return {"orderId": "sl"}

        def get_order(self, *args):
            return {"orderId": "entry"}

        def open_orders(self, *args):
            return []

        def account(self):
            return {"ZUSD": "1"}

    class FakeRules:
        def __init__(self, *args, **kwargs):
            pass

        def quantity_from_quote(self, *args, **kwargs):
            return "0.1"

        def normalize_exit_price(self, symbol, price):
            return str(price)

        def base_asset(self, symbol):
            return "BTC"

        def normalize_exit_quantity(self, symbol, qty):
            return qty

        def oco_allowed(self, symbol):
            return False

        def symbol_info(self, symbol):
            return {"quoteAsset": "USDC"}

    class FakeMargin:
        def __init__(self, *args, **kwargs):
            pass

        def ensure_isolated_account(self, symbol): return {"status": "ok"}
        def isolated_account(self, symbol): return {"assets": []}
        def borrow(self, *args): return {"status": "ok"}
        def repay(self, *args): return {"status": "ok"}
        def transfer_spot_to_margin(self, *args): return {"status": "ok"}
        def margin_order(self, *args, **kwargs): return {"orderId": "margin"}
        def open_margin_orders(self, symbol): return []

    monkeypatch.setattr(kraken_full_smoke_test, "KrakenClient", FakeClient)
    monkeypatch.setattr(kraken_full_smoke_test, "KrakenSymbolRules", FakeRules)
    monkeypatch.setattr(kraken_full_smoke_test, "KrakenMarginClient", FakeMargin)

    result = kraken_full_smoke_test.run_smoke(SimpleNamespace(symbol="BTCUSDC", base_url=None, order_quote=20.0, skip_private=False, validate_order=False))

    assert result.credentials_loaded is True
    assert result.credential_sources["api_key_loaded"] is True
    assert result.credential_sources["runtime_env_api_key_loaded"] is True
    assert any(check["name"] == "private_account" and check["ok"] for check in result.checks)
    assert not any(check.get("reason") == "missing_kraken_api_credentials" for check in result.checks)


def test_admin_kraken_status_uses_safe_test_endpoint(monkeypatch):
    calls = []

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {
                "status": "ok",
                "base_url": "https://api.kraken.com",
                "api_key_loaded": True,
                "secret_key_loaded": True,
                "account_keys": ["ZUSD"],
            }

    def fake_post(url, timeout):
        calls.append((url, timeout))
        return FakeResponse()

    monkeypatch.setattr(kraken_full_smoke_test.requests, "post", fake_post)

    status = kraken_full_smoke_test._fetch_admin_kraken_credential_status("https://signalmaker.test", timeout=2.5)

    assert calls == [("https://signalmaker.test/api/v1/admin/test/kraken", 2.5)]
    assert status == {
        "checked": True,
        "status": "ok",
        "base_url": "https://api.kraken.com",
        "api_key_loaded": True,
        "secret_key_loaded": True,
        "error": None,
        "http_status": None,
    }


def test_missing_local_credentials_reports_when_admin_has_credentials(monkeypatch):
    file_settings = _settings()
    monkeypatch.setattr(kraken_full_smoke_test, "ensure_env", lambda: None)
    monkeypatch.setattr(kraken_full_smoke_test, "load_settings", lambda: file_settings)
    monkeypatch.setattr(kraken_full_smoke_test, "apply_admin_settings_to_environ", lambda base_url: {"applied": True})
    monkeypatch.setattr(
        kraken_full_smoke_test,
        "_fetch_admin_kraken_credential_status",
        lambda base_url: {"checked": True, "api_key_loaded": True, "secret_key_loaded": True, "status": "ok"},
    )
    monkeypatch.setattr(kraken_full_smoke_test, "fetch_kraken_ohlc", lambda *args, **kwargs: [1, 2, 3])
    monkeypatch.setattr(kraken_full_smoke_test, "discover_kraken_spot_symbols", lambda *args, **kwargs: ["BTCUSDC"])
    monkeypatch.setattr(kraken_full_smoke_test, "discover_kraken_margin_symbols", lambda *args, **kwargs: ["BTCUSDC"])

    class FakeClient:
        def __init__(self, base_url, api_key, secret_key, dry_run=True):
            self.api_key = api_key
            self.secret_key = secret_key

        def is_configured(self):
            return False

        def _public(self, path): return {"unixtime": 1}
        def _pair_info(self, symbol): return {"altname": symbol}
        def current_price(self, symbol): return 100.0
        def place_market_entry(self, *args): return {"orderId": "entry"}
        def place_exit_limit(self, *args): return {"orderId": "tp"}
        def place_stop_loss(self, *args): return {"orderId": "sl"}
        def get_order(self, *args): return {"orderId": "entry"}
        def open_orders(self, *args): return []

    class FakeRules:
        def __init__(self, *args, **kwargs): pass
        def quantity_from_quote(self, *args, **kwargs): return "0.1"
        def normalize_exit_price(self, symbol, price): return str(price)
        def base_asset(self, symbol): return "BTC"
        def normalize_exit_quantity(self, symbol, qty): return qty
        def oco_allowed(self, symbol): return False
        def symbol_info(self, symbol): return {"quoteAsset": "USDC"}

    class FakeMargin:
        def __init__(self, *args, **kwargs): pass
        def ensure_isolated_account(self, symbol): return {"status": "ok"}
        def isolated_account(self, symbol): return {"assets": []}
        def borrow(self, *args): return {"status": "ok"}
        def repay(self, *args): return {"status": "ok"}
        def transfer_spot_to_margin(self, *args): return {"status": "ok"}
        def margin_order(self, *args, **kwargs): return {"orderId": "margin"}
        def open_margin_orders(self, symbol): return []

    monkeypatch.setattr(kraken_full_smoke_test, "KrakenClient", FakeClient)
    monkeypatch.setattr(kraken_full_smoke_test, "KrakenSymbolRules", FakeRules)
    monkeypatch.setattr(kraken_full_smoke_test, "KrakenMarginClient", FakeMargin)

    result = kraken_full_smoke_test.run_smoke(SimpleNamespace(symbol="BTCUSDC", base_url=None, order_quote=20.0, skip_private=False, validate_order=False))

    assert result.credentials_loaded is False
    assert result.credential_sources["admin_kraken_test"]["api_key_loaded"] is True
    assert any(
        check.get("reason") == "missing_local_kraken_api_credentials_admin_has_credentials"
        for check in result.checks
    )


def test_settings_with_runtime_db_credentials_override_env(monkeypatch):
    monkeypatch.setenv("KRAKEN_API_KEY", "env-key")
    monkeypatch.setenv("KRAKEN_SECRET_KEY", "env-secret")
    runtime = {"kraken": {"kraken_api_key": "db-key", "kraken_secret_key": "db-secret", "kraken_base_url": "https://db.kraken/"}}

    settings = kraken_full_smoke_test._settings_with_runtime_overrides(_settings(), runtime)

    assert settings.kraken_api_key == "db-key"
    assert settings.kraken_secret_key == "db-secret"
    assert settings.kraken_base_url == "https://db.kraken"


def test_credential_sources_reports_selected_database(monkeypatch):
    monkeypatch.setattr(kraken_full_smoke_test, "load_settings", lambda: _settings(kraken_api_key="file-key", kraken_secret_key="file-secret"))
    runtime = {"kraken": {"kraken_api_key": "db-key", "kraken_secret_key": "db-secret"}}

    sources = kraken_full_smoke_test._credential_sources(
        _settings(kraken_api_key="db-key", kraken_secret_key="db-secret"), {}, {}, runtime
    )

    assert sources["db_kraken_api_key_loaded"] == {"loaded": True, "length": 6}
    assert sources["db_kraken_secret_key_loaded"] == {"loaded": True, "length": 9}
    assert sources["selected_source"] == "database canonical lowercase"


def test_admin_kraken_status_falls_back_to_get_on_405(monkeypatch):
    calls = []

    class Post405:
        status_code = 405

        def raise_for_status(self):
            raise AssertionError("POST 405 should be retried with GET before raise_for_status")

    class GetOk:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return {"status": "ok", "api_key_loaded": True, "secret_key_loaded": True}

    def fake_post(url, timeout):
        calls.append(("POST", url, timeout))
        return Post405()

    def fake_get(url, timeout):
        calls.append(("GET", url, timeout))
        return GetOk()

    monkeypatch.setattr(kraken_full_smoke_test.requests, "post", fake_post)
    monkeypatch.setattr(kraken_full_smoke_test.requests, "get", fake_get)

    status = kraken_full_smoke_test._fetch_admin_kraken_credential_status("https://signalmaker.test", timeout=2.5)

    assert calls == [
        ("POST", "https://signalmaker.test/api/v1/admin/test/kraken", 2.5),
        ("GET", "https://signalmaker.test/api/v1/admin/test/kraken", 2.5),
    ]
    assert status["checked"] is True
    assert status["api_key_loaded"] is True
    assert status["secret_key_loaded"] is True

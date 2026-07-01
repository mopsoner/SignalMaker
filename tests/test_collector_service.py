import pytest

from app.services import collector_service
from app.services.collector_service import CollectorService


RUNTIME = {
    "kraken": {"kraken_base_url": "https://kraken.test"},
    "market_data": {
        "kraken_collector_enabled": True,
        "kraken_quote_assets": "USD,USDC,USDT",
        "kraken_excluded_base_assets": "EUR",
        "kraken_symbol_status": "TRADING",
        "kraken_max_symbols": 10,
    },
}


class FakeResponse:
    status_code = 200
    headers = {}

    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return FakeResponse(self.payload)


def make_service(monkeypatch, payload):
    monkeypatch.setattr(collector_service, "load_runtime_settings", lambda: RUNTIME)
    service = CollectorService()
    service.session = FakeSession(payload)
    return service


def test_discover_symbols_maps_kraken_asset_pairs_to_internal_symbols(monkeypatch):
    payload = {
        "error": [],
        "result": {
            "XXBTZUSD": {
                "altname": "XBTUSD",
                "wsname": "XBT/USD",
                "base": "XXBT",
                "quote": "ZUSD",
                "status": "online",
            },
            "XBTUSDC": {
                "altname": "XBTUSDC",
                "wsname": "XBT/USDC",
                "base": "XXBT",
                "quote": "USDC",
                "status": "online",
            },
            "ETHUSDT": {
                "altname": "ETHUSDT",
                "wsname": "ETH/USDT",
                "base": "XETH",
                "quote": "USDT",
                "status": "online",
            },
        },
    }
    service = make_service(monkeypatch, payload)

    symbols = service.discover_symbols()

    assert symbols == ["BTCUSD", "BTCUSDC", "ETHUSDT"]
    assert service.session.calls == [
        {
            "url": "https://kraken.test/0/public/AssetPairs",
            "params": {"assetVersion": 1},
            "timeout": 15,
        }
    ]


def test_discover_symbols_filters_kraken_status_quote_base_and_darkpool(monkeypatch):
    payload = {
        "error": [],
        "result": {
            "XXBTZUSD": {"base": "XXBT", "quote": "ZUSD", "status": "online"},
            "XLTCZEUR": {"base": "XLTC", "quote": "ZEUR", "status": "online"},
            "XETHZUSD": {"base": "XETH", "quote": "ZUSD", "status": "cancel_only"},
            "EURUSDC": {"base": "ZEUR", "quote": "USDC", "status": "online"},
            "XXBTZUSD.d": {"base": "XXBT", "quote": "ZUSD", "status": "online"},
        },
    }
    service = make_service(monkeypatch, payload)

    assert service.discover_symbols() == ["BTCUSD"]


def test_get_rejects_binance_api_paths_for_kraken(monkeypatch):
    service = make_service(monkeypatch, {"error": [], "result": {}})

    with pytest.raises(RuntimeError, match="Binance endpoint"):
        service._get("/api/v3/exchangeInfo")

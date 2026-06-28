from types import SimpleNamespace

from raspberry_executor import candle_auto_feed


class FakeResponse:
    ok = True

    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def kraken_asset_pairs_payload():
    return {
        "error": [],
        "result": {
            "XXBTZUSD": {"altname": "XBTUSD", "base": "XBT", "quote": "ZUSD", "status": "online", "leverage_buy": [2, 3, 4, 5], "leverage_sell": [2, 3, 4, 5]},
            "ETHUSDC": {"altname": "ETHUSDC", "base": "ETH", "quote": "USDC", "status": "online", "leverage_buy": [2, 3], "leverage_sell": [2, 3]},
            "ADAUSDC": {"altname": "ADAUSDC", "base": "ADA", "quote": "USDC", "status": "online", "leverage_buy": [], "leverage_sell": []},
            "SOLUSDT": {"altname": "SOLUSDT", "base": "SOL", "quote": "USDT", "status": "online", "leverage_buy": [2], "leverage_sell": [2]},
            "OFFUSDC": {"altname": "OFFUSDC", "base": "OFF", "quote": "USDC", "status": "cancel_only", "leverage_buy": [2], "leverage_sell": [2]},
        },
    }


def test_discover_kraken_margin_symbols_keeps_only_margin_tradeable_quotes(monkeypatch):
    monkeypatch.setattr(candle_auto_feed.requests, "get", lambda *args, **kwargs: FakeResponse(kraken_asset_pairs_payload()))

    assert candle_auto_feed.discover_kraken_margin_symbols("https://kraken.test", ["USDC"]) == ["ETHUSDC"]


def test_discover_symbols_for_exchange_uses_kraken_margin_filter_for_cross(monkeypatch):
    monkeypatch.setattr(candle_auto_feed.requests, "get", lambda *args, **kwargs: FakeResponse(kraken_asset_pairs_payload()))
    settings = SimpleNamespace(exchange="kraken", kraken_base_url="https://kraken.test", binance_base_url="https://binance.test", binance_api_key="")

    symbols, source = candle_auto_feed.discover_symbols_for_exchange(settings, ["USDC"], "cross")

    assert symbols == ["ETHUSDC"]
    assert source == "kraken_margin"


def test_discover_kraken_spot_symbols_can_include_non_margin_spot_pairs(monkeypatch):
    monkeypatch.setattr(candle_auto_feed.requests, "get", lambda *args, **kwargs: FakeResponse(kraken_asset_pairs_payload()))

    assert candle_auto_feed.discover_kraken_spot_symbols("https://kraken.test", ["USDC"]) == ["ADAUSDC", "ETHUSDC"]

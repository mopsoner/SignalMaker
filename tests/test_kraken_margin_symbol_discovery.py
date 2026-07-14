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
            "CAKEUSD": {"altname": "CAKEUSD", "base": "CAKE", "quote": "ZUSD", "status": "online", "leverage_buy": [2, 3], "leverage_sell": []},
            "MELANIAUSD": {"altname": "MELANIAUSD", "base": "MELANIA", "quote": "ZUSD", "status": "online", "leverage_buy": [2, 3], "leverage_sell": []},
            "XBTUSDC": {"altname": "XBTUSDC", "base": "XXBT", "quote": "USDC", "status": "online", "leverage_buy": [2, 3], "leverage_sell": [2, 3]},
            "ETHUSDC": {"altname": "ETHUSDC", "base": "ETH", "quote": "USDC", "status": "online", "leverage_buy": [2, 3], "leverage_sell": [2, 3]},
            "CAKEUSDC": {"altname": "CAKEUSDC", "base": "CAKE", "quote": "USDC", "status": "online", "leverage_buy": [2, 3], "leverage_sell": []},
            "ADAUSDC": {"altname": "ADAUSDC", "base": "ADA", "quote": "USDC", "status": "online", "leverage_buy": [], "leverage_sell": []},
            "SOLUSDT": {"altname": "SOLUSDT", "base": "SOL", "quote": "USDT", "status": "online", "leverage_buy": [2], "leverage_sell": [2]},
            "OFFUSDC": {"altname": "OFFUSDC", "base": "OFF", "quote": "USDC", "status": "cancel_only", "leverage_buy": [2], "leverage_sell": [2]},
        },
    }


def test_discover_kraken_margin_symbols_keeps_margin_buy_quotes(monkeypatch):
    monkeypatch.delenv("CANDLE_FEED_REQUIRE_MARGIN_SELL", raising=False)
    monkeypatch.setattr(candle_auto_feed, "read_env", lambda: {})
    monkeypatch.setattr(candle_auto_feed.requests, "get", lambda *args, **kwargs: FakeResponse(kraken_asset_pairs_payload()))

    assert candle_auto_feed.discover_kraken_margin_symbols("https://kraken.test", ["USDC"]) == ["BTCUSDC", "CAKEUSDC", "ETHUSDC"]


def test_discover_kraken_margin_symbols_includes_usd_long_only_pairs(monkeypatch):
    monkeypatch.delenv("CANDLE_FEED_REQUIRE_MARGIN_SELL", raising=False)
    monkeypatch.setattr(candle_auto_feed, "read_env", lambda: {})
    monkeypatch.setattr(candle_auto_feed.requests, "get", lambda *args, **kwargs: FakeResponse(kraken_asset_pairs_payload()))

    symbols = candle_auto_feed.discover_kraken_margin_symbols("https://kraken.test", ["USD"])

    assert "CAKEUSD" in symbols
    assert "MELANIAUSD" in symbols

def test_discover_kraken_margin_symbols_can_require_margin_sell(monkeypatch):
    monkeypatch.setenv("CANDLE_FEED_REQUIRE_MARGIN_SELL", "true")
    monkeypatch.setattr(candle_auto_feed, "read_env", lambda: {})
    monkeypatch.setattr(candle_auto_feed.requests, "get", lambda *args, **kwargs: FakeResponse(kraken_asset_pairs_payload()))

    assert candle_auto_feed.discover_kraken_margin_symbols("https://kraken.test", ["USDC"]) == ["BTCUSDC", "ETHUSDC"]


def test_discover_symbols_for_exchange_keeps_spot_universe_for_cross(monkeypatch):
    monkeypatch.setenv("CANDLE_FEED_MARGIN_ONLY", "false")
    monkeypatch.setattr(candle_auto_feed, "read_env", lambda: {"CANDLE_FEED_MARGIN_ONLY": "false"})
    monkeypatch.setattr(candle_auto_feed.requests, "get", lambda *args, **kwargs: FakeResponse(kraken_asset_pairs_payload()))
    settings = SimpleNamespace(exchange="kraken", kraken_base_url="https://kraken.test", kraken_api_key="")

    symbols, source = candle_auto_feed.discover_symbols_for_exchange(settings, ["USDC"], "cross")

    assert symbols == ["ADAUSDC", "BTCUSDC", "CAKEUSDC", "ETHUSDC"]
    assert source == "kraken_spot"


def test_discover_kraken_spot_symbols_can_include_non_margin_spot_pairs(monkeypatch):
    monkeypatch.setattr(candle_auto_feed.requests, "get", lambda *args, **kwargs: FakeResponse(kraken_asset_pairs_payload()))

    assert candle_auto_feed.discover_kraken_spot_symbols("https://kraken.test", ["USDC"]) == ["ADAUSDC", "BTCUSDC", "CAKEUSDC", "ETHUSDC"]

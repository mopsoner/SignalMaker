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


def test_process_pair_uses_kraken_ohlc_and_pushes_missing_remote_candles(monkeypatch):
    calls = {}

    class FakeClient:
        def latest_candle(self, symbol, interval):
            return {"open_time": 1_000, "close_time": 1_999}

        def post_candles(self, symbol, interval, candles, source):
            calls["post"] = {"symbol": symbol, "interval": interval, "candles": candles, "source": source}
            return {"upserted": len(candles)}

    def fake_fetch(exchange, base_url, symbol, interval, limit, start_time=None):
        calls["fetch"] = {
            "exchange": exchange,
            "base_url": base_url,
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
            "start_time": start_time,
        }
        return [
            {"open_time": 1_000, "close_time": 1_999},
            {"open_time": 2_000, "close_time": 2_999},
        ]

    monkeypatch.setattr(candle_auto_feed, "fetch_exchange_klines", fake_fetch)
    settings = SimpleNamespace(exchange="kraken", kraken_base_url="https://kraken.test", binance_base_url="https://binance.test", gateway_id="raspberry-test")

    result = candle_auto_feed._process_pair(settings, FakeClient(), candle_auto_feed.RateLimiter(10_000), "ETHUSDC", "1h", 50)

    assert calls["fetch"] == {
        "exchange": "kraken",
        "base_url": "https://kraken.test",
        "symbol": "ETHUSDC",
        "interval": "1h",
        "limit": 50,
        "start_time": 2_000,
    }
    assert calls["post"] == {
        "symbol": "ETHUSDC",
        "interval": "1h",
        "candles": [{"open_time": 2_000, "close_time": 2_999}],
        "source": "raspberry-test",
    }
    assert result == {"kind": "pushed", "symbol": "ETHUSDC", "interval": "1h", "count": 1, "start_time": 2_000, "upserted": 1}


def test_effective_candle_requests_per_minute_uses_kraken_limit():
    settings = SimpleNamespace(exchange="kraken", kraken_base_url="https://kraken.test", binance_base_url="https://binance.test")

    assert candle_auto_feed.effective_candle_requests_per_minute(settings, {"CANDLE_FEED_KRAKEN_REQUESTS_PER_MINUTE": "42"}) == (42, 42, 1.0, "kraken")

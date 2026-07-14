import importlib.util
import sys
from pathlib import Path


SPEC = importlib.util.spec_from_file_location(
    "bootstrap_wyckoff_candles",
    Path(__file__).resolve().parents[1] / "scripts" / "bootstrap_wyckoff_candles.py",
)
bootstrap = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = bootstrap
assert SPEC.loader is not None
SPEC.loader.exec_module(bootstrap)


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
            "SHORTUSDC": {"altname": "SHORTUSDC", "base": "SHORT", "quote": "USDC", "status": "online", "leverage_buy": [], "leverage_sell": [2, 3]},
            "ADAUSDC": {"altname": "ADAUSDC", "base": "ADA", "quote": "USDC", "status": "online", "leverage_buy": [], "leverage_sell": []},
            "OFFUSDC": {"altname": "OFFUSDC", "base": "OFF", "quote": "USDC", "status": "cancel_only", "leverage_buy": [2], "leverage_sell": [2]},
        },
    }


def test_bootstrap_margin_assets_match_candle_feed_long_margin_discovery(monkeypatch):
    seen_urls = []

    def fake_http_json(url, **kwargs):
        seen_urls.append(url)
        return kraken_asset_pairs_payload()

    monkeypatch.delenv("BOOTSTRAP_REQUIRE_MARGIN_SELL", raising=False)
    monkeypatch.delenv("CANDLE_FEED_REQUIRE_MARGIN_SELL", raising=False)
    monkeypatch.setattr(bootstrap, "http_json", fake_http_json)

    pairs = bootstrap.load_kraken_pairs({"USDC"}, margin_only=True, max_symbols=0)

    assert [pair.symbol for pair in pairs] == ["BTCUSDC", "CAKEUSDC", "ETHUSDC"]
    assert "assetVersion=1" in seen_urls[0]


def test_bootstrap_margin_assets_can_require_margin_sell(monkeypatch):
    monkeypatch.setenv("BOOTSTRAP_REQUIRE_MARGIN_SELL", "true")
    monkeypatch.setattr(bootstrap, "http_json", lambda *args, **kwargs: kraken_asset_pairs_payload())

    pairs = bootstrap.load_kraken_pairs({"USDC"}, margin_only=True, max_symbols=0)

    assert [pair.symbol for pair in pairs] == ["BTCUSDC", "ETHUSDC"]

from types import SimpleNamespace

import pytest

from raspberry_executor.exchange_factory import create_margin_exchange, create_spot_exchange, exchange_name
from raspberry_executor.kraken_client import KrakenClient
from raspberry_executor.kraken_margin_client import KrakenMarginClient
from raspberry_executor.kraken_symbol_rules import KrakenSymbolRules


def settings(exchange="kraken"):
    return SimpleNamespace(
        exchange=exchange,
        dry_run=True,
        quote_assets=["USDC"],
        kraken_base_url="https://kraken.test",
        kraken_api_key="kraken-key",
        kraken_secret_key="kraken-secret",
    )


def test_exchange_name_defaults_to_kraken_for_device_executor():
    assert exchange_name(SimpleNamespace()) == "kraken"


def test_create_spot_exchange_can_select_kraken_adapter():
    client, rules = create_spot_exchange(settings("kraken"))

    assert isinstance(client, KrakenClient)
    assert isinstance(rules, KrakenSymbolRules)
    assert client.base_url == "https://kraken.test"
    assert client.api_key == "kraken-key"
    assert rules.quote_assets == ["USDC"]


def test_create_spot_exchange_rejects_unknown_exchange():
    with pytest.raises(RuntimeError, match="unsupported_execution_exchange"):
        create_spot_exchange(settings("unknown"))


def test_create_margin_exchange_can_select_kraken_cross_margin_adapter():
    client, margin, rules = create_margin_exchange(settings("kraken"), isolated=True, dry_run=False)

    assert isinstance(client, KrakenClient)
    assert isinstance(margin, KrakenMarginClient)
    assert isinstance(rules, KrakenSymbolRules)
    assert margin.isolated is False
    assert margin.requested_isolated is True
    assert margin.ensure_isolated_account("BTCUSDC")["status"] == "cross_margin_required"


def test_kraken_margin_borrow_and_repay_are_recorded_as_implicit_operations():
    client, margin, _ = create_margin_exchange(settings("kraken"), isolated=False, dry_run=True)

    borrow = margin.borrow("BTCUSDC", "USDC", "10")
    repay = margin.repay("BTCUSDC", "USDC", "10")

    assert borrow["status"] == "implicit_borrow_on_order"
    assert repay["status"] == "implicit_repay_on_close_or_settle"
    assert client.dry_run is True


def public_methods(cls):
    return {name for name, value in cls.__dict__.items() if callable(value) and not name.startswith("_")}


def test_kraken_client_covers_kraken_client_public_surface():
    from raspberry_executor.kraken_client import KrakenClient

    assert public_methods(KrakenClient) - public_methods(KrakenClient) == set()


def test_kraken_symbol_rules_covers_kraken_symbol_rules_public_surface():
    from raspberry_executor.kraken_symbol_rules import KrakenSymbolRules

    assert public_methods(KrakenSymbolRules) - public_methods(KrakenSymbolRules) == set()


def test_kraken_margin_client_covers_kraken_margin_client_public_surface():
    from raspberry_executor.margin_client import MarginClient

    assert public_methods(MarginClient) - public_methods(KrakenMarginClient) == set()


def test_kraken_client_resolves_btc_alias_to_xbt_pair():
    client = KrakenClient("https://kraken.test", "", "", dry_run=True)
    client._public = lambda *args, **kwargs: {  # type: ignore[method-assign]
        "XBTUSDC": {
            "altname": "XBTUSDC",
            "wsname": "XBT/USDC",
            "base": "XXBT",
            "quote": "USDC",
            "lot_decimals": 8,
        }
    }

    info = client._pair_info("BTCUSDC")

    assert info["pair_key"] == "XBTUSDC"
    assert info["baseAsset"] == "BTC"
    assert info["quoteAsset"] == "USDC"


def test_kraken_symbol_rules_resolves_btc_alias_to_xbt_pair(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "error": [],
                "result": {
                    "XBTUSDC": {
                        "altname": "XBTUSDC",
                        "wsname": "XBT/USDC",
                        "base": "XXBT",
                        "quote": "USDC",
                        "lot_decimals": 8,
                    }
                },
            }

    import raspberry_executor.kraken_symbol_rules as kraken_symbol_rules

    monkeypatch.setattr(kraken_symbol_rules.requests, "get", lambda *args, **kwargs: FakeResponse())
    rules = KrakenSymbolRules("https://kraken.test", quote_assets=["USDC"])

    info = rules.symbol_info("BTCUSDC")

    assert info["pair_key"] == "XBTUSDC"
    assert info["baseAsset"] == "BTC"
    assert info["quoteAsset"] == "USDC"


def test_kraken_client_keeps_usdt_distinct_from_usd_pair():
    client = KrakenClient("https://kraken.test", "", "", dry_run=True)
    client._public = lambda *args, **kwargs: {  # type: ignore[method-assign]
        "XXBTZUSD": {"altname": "XBTUSD", "wsname": "XBT/USD", "base": "XXBT", "quote": "ZUSD"},
        "XBTUSDT": {"altname": "XBTUSDT", "wsname": "XBT/USDT", "base": "XXBT", "quote": "USDT"},
    }

    info = client._pair_info("BTCUSDT")

    assert info["pair_key"] == "XBTUSDT"
    assert info["quoteAsset"] == "USDT"

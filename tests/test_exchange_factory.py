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
        binance_base_url="https://binance.test",
        binance_api_key="binance-key",
        binance_secret_key="binance-secret",
        kraken_base_url="https://kraken.test",
        kraken_api_key="kraken-key",
        kraken_secret_key="kraken-secret",
    )


def test_exchange_name_defaults_to_binance_for_backward_compatibility():
    assert exchange_name(SimpleNamespace()) == "binance"


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


def test_kraken_client_covers_binance_client_public_surface():
    from raspberry_executor.binance_client import BinanceClient

    assert public_methods(BinanceClient) - public_methods(KrakenClient) == set()


def test_kraken_symbol_rules_covers_binance_symbol_rules_public_surface():
    from raspberry_executor.binance_symbol_rules import BinanceSymbolRules

    assert public_methods(BinanceSymbolRules) - public_methods(KrakenSymbolRules) == set()


def test_kraken_margin_client_covers_binance_margin_client_public_surface():
    from raspberry_executor.margin_client import MarginClient

    assert public_methods(MarginClient) - public_methods(KrakenMarginClient) == set()

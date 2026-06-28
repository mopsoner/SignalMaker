from types import SimpleNamespace

from raspberry_executor import exchange_smoke_test


def test_probe_symbol_uses_kraken_quote_asset():
    settings = SimpleNamespace(quote_assets=["USDC"])
    assert exchange_smoke_test._probe_symbol(settings, "kraken") == "BTCUSDC"


def test_probe_symbol_defaults_binance_to_btcusdt():
    settings = SimpleNamespace(quote_assets=["USDT"])
    assert exchange_smoke_test._probe_symbol(settings, "binance") == "BTCUSDT"

from unittest.mock import Mock

import pytest

from app.services import kraken_candle_importer as importer


def _pair(*, leverage_buy=None, leverage_sell=None):
    return {
        "altname": "XBTUSD",
        "wsname": "XBT/USD",
        "base": "XXBT",
        "quote": "ZUSD",
        "status": "online",
        "leverage_buy": leverage_buy or [],
        "leverage_sell": leverage_sell or [],
    }


def test_discover_margin_pairs_includes_long_only_by_default(monkeypatch):
    monkeypatch.setattr(
        importer,
        "fetch_kraken_asset_pairs",
        lambda _base_url: {"XXBTZUSD": _pair(leverage_buy=[2, 3])},
    )

    pairs = importer.discover_kraken_pairs(quote_assets=["USD"])

    assert [pair.symbol for pair in pairs] == ["BTCUSD"]


def test_discover_margin_pairs_can_require_sell_leverage(monkeypatch):
    monkeypatch.setattr(
        importer,
        "fetch_kraken_asset_pairs",
        lambda _base_url: {"XXBTZUSD": _pair(leverage_buy=[2, 3])},
    )

    pairs = importer.discover_kraken_pairs(
        quote_assets=["USD"],
        require_margin_sell=True,
    )

    assert pairs == []


def test_fetch_ohlc_normalizes_candles_and_provider_symbol(monkeypatch):
    response = Mock()
    response.json.return_value = {
        "error": [],
        "result": {
            "XXBTZUSD": [[1_700_000_000, "1", "3", "0.5", "2", "1.5", "4", 7]],
            "last": 1_700_000_900,
        },
    }
    monkeypatch.setattr(importer.requests, "get", Mock(return_value=response))
    pair = importer.KrakenPair("XXBTZUSD", "XBTUSD", "XBT/USD", "BTC", "USD", "BTCUSD", [2], [])

    candles = importer.fetch_kraken_ohlc(pair=pair, interval="15m", limit=10)

    response.raise_for_status.assert_called_once_with()
    assert candles == [
        {
            "open_time": 1_700_000_000_000,
            "close_time": 1_700_000_899_999,
            "open": 1.0,
            "high": 3.0,
            "low": 0.5,
            "close": 2.0,
            "volume": 4.0,
            "quote_volume": 6.0,
            "number_of_trades": 7,
            "taker_buy_base_volume": 0.0,
            "taker_buy_quote_volume": 0.0,
            "provider": "KRAKEN",
            "provider_symbol": "XBTUSD",
            "exchange": "kraken",
        }
    ]


def test_import_writes_candles_directly_through_market_data_service(monkeypatch):
    pair = importer.KrakenPair("XXBTZUSD", "XBTUSD", "XBT/USD", "BTC", "USD", "BTCUSD", [2], [])
    monkeypatch.setattr(importer, "discover_kraken_pairs", Mock(return_value=[pair]))
    monkeypatch.setattr(importer, "fetch_kraken_ohlc", Mock(return_value=[{"open_time": 1000}]))
    service = Mock()
    service.list_candles.return_value = []
    service.upsert_candles.return_value = 1
    service_factory = Mock(return_value=service)
    monkeypatch.setattr(importer, "MarketDataService", service_factory)
    db = Mock()

    result = importer.import_kraken_candles(db=db, intervals=["15m"], requests_per_minute=60)

    service_factory.assert_called_once_with(db)
    service.upsert_candles.assert_called_once_with("BTCUSD", "15m", [{"open_time": 1000}])
    assert result["status"] == "ok"
    assert result["source"] == "kraken_internal"
    assert result["pushed_count"] == 1


def test_fetch_ohlc_rejects_unsupported_interval():
    pair = importer.KrakenPair("XXBTZUSD", "XBTUSD", "XBT/USD", "BTC", "USD", "BTCUSD", [2], [])

    with pytest.raises(ValueError, match="Unsupported interval"):
        importer.fetch_kraken_ohlc(pair=pair, interval="2h", limit=10)

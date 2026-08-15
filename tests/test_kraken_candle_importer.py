from types import SimpleNamespace
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
    monkeypatch.setattr(importer.time, "time", lambda: 10)
    candle = {"open_time": 1000, "close_time": 1999}
    monkeypatch.setattr(importer, "fetch_kraken_ohlc", Mock(return_value=[candle]))
    service = Mock()
    service.list_candles.return_value = []
    service.upsert_candles.return_value = 1
    service_factory = Mock(return_value=service)
    monkeypatch.setattr(importer, "MarketDataService", service_factory)
    db = Mock()

    result = importer.import_kraken_candles(db=db, intervals=["15m"], requests_per_minute=60)

    service_factory.assert_called_once_with(db)
    service.upsert_candles.assert_called_once_with("BTCUSD", "15m", [candle])
    assert result["status"] == "ok"
    assert result["source"] == "kraken_internal"
    assert result["pushed_count"] == 1


def test_fetch_ohlc_rejects_unsupported_interval():
    pair = importer.KrakenPair("XXBTZUSD", "XBTUSD", "XBT/USD", "BTC", "USD", "BTCUSD", [2], [])

    with pytest.raises(ValueError, match="Unsupported interval"):
        importer.fetch_kraken_ohlc(pair=pair, interval="2h", limit=10)


class _PersistingService:
    def __init__(self, rows):
        self.rows = {row["open_time"]: row for row in rows}

    def list_candles(self, **_kwargs):
        if not self.rows:
            return []
        row = self.rows[max(self.rows)]
        return [SimpleNamespace(**row)]

    def upsert_candles(self, _symbol, _interval, rows):
        self.rows.update({row["open_time"]: row for row in rows})
        return len(rows)


def _candle(open_time, step=900_000):
    return {
        "open_time": open_time,
        "close_time": open_time + step - 1,
        "open": 1,
        "high": 2,
        "low": 0.5,
        "close": 1.5,
        "volume": 1,
    }


def _run_catchup(monkeypatch, service, fetch, *, now, limit=3):
    pair = importer.KrakenPair("XXBTZUSD", "XBTUSD", "XBT/USD", "BTC", "USD", "BTCUSD", [2], [])
    monkeypatch.setattr(importer, "discover_kraken_pairs", lambda **_kwargs: [pair])
    monkeypatch.setattr(importer, "MarketDataService", lambda _db: service)
    monkeypatch.setattr(importer, "fetch_kraken_ohlc", fetch)
    monkeypatch.setattr(importer.time, "time", lambda: now / 1000)
    return importer.import_kraken_candles(db=Mock(), intervals=["15m"], limit=limit, requests_per_minute=10**9)


def test_catchup_two_day_gap_is_paginated_without_current_candle(monkeypatch):
    step = 900_000
    all_rows = [_candle(i * step) for i in range(194)]
    service = _PersistingService(all_rows[:1])

    def fetch(**kwargs):
        since = kwargs["since_ms"]
        # Simulate an inclusive boundary candle returned by the provider.
        eligible = [row for row in all_rows if row["open_time"] >= since - step]
        return eligible[: kwargs["limit"]]

    result = _run_catchup(monkeypatch, service, fetch, now=193 * step + step // 2, limit=50)

    assert result["pushed"][0]["pages"] > 1
    assert max(service.rows) == 192 * step
    # The real validator is independent of storage and proves duplicate page
    # boundaries did not leave a gap in the persisted series.
    from app.services.market_data_service import MarketDataService
    validation = MarketDataService.__new__(MarketDataService).validate_candle_series("15m", list(service.rows.values()))
    assert validation["gap_count"] == 0


def test_catchup_resumes_after_interruption_mid_pagination(monkeypatch):
    step = 900_000
    all_rows = [_candle(i * step) for i in range(12)]
    service = _PersistingService(all_rows[:1])
    calls = 0

    def interrupted(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("temporary outage")
        return [row for row in all_rows if row["open_time"] >= kwargs["since_ms"]][: kwargs["limit"]]

    first = _run_catchup(monkeypatch, service, interrupted, now=12 * step, limit=3)
    persisted_after_failure = max(service.rows)
    assert first["status"] == "partial"
    assert persisted_after_failure == 3 * step

    def resumed(**kwargs):
        # Include the previous boundary to exercise idempotent overlap.
        return [row for row in all_rows if row["open_time"] >= kwargs["since_ms"] - step][: kwargs["limit"]]

    second = _run_catchup(monkeypatch, service, resumed, now=12 * step, limit=3)
    from app.services.market_data_service import MarketDataService
    validation = MarketDataService.__new__(MarketDataService).validate_candle_series("15m", list(service.rows.values()))
    assert second["status"] == "ok"
    assert validation["gap_count"] == 0

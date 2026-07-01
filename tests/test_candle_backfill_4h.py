from types import SimpleNamespace

from raspberry_executor import candle_backfill_4h


class NoSleepLimiter:
    def wait(self):
        return None


def test_run_once_backfills_history_before_recent_first_remote_candle(monkeypatch, tmp_path):
    posted = []

    settings = SimpleNamespace(
        signalmaker_base_url="https://signalmaker.test",
        gateway_id="gw-test",
        kraken_base_url="https://kraken.test",
        exchange="kraken",
        quote_assets=["USD"],
        allowed_symbols=["BTCUSD"],
    )

    class FakeSignalMakerClient:
        def __init__(self, base_url, gateway_id):
            self.base_url = base_url
            self.gateway_id = gateway_id

        def check_candle_ingest_endpoint(self):
            return {"ok": True}

        def first_candle(self, symbol, interval):
            assert (symbol, interval) == ("BTCUSD", "4h")
            return {"symbol": symbol, "interval": interval, "open_time": 3000}

        def post_candles(self, symbol, interval, candles, source=None):
            posted.append({"symbol": symbol, "interval": interval, "candles": candles, "source": source})
            return {"status": "ok", "upserted": len(candles)}

    def fake_fetch(exchange, base_url, symbol, interval, limit, start_time=None):
        assert start_time == 1000
        return [
            {"open_time": 1000, "close_time": 1999, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
            {"open_time": 2000, "close_time": 2999, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
            {"open_time": 3000, "close_time": 3999, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
        ]

    monkeypatch.setattr(candle_backfill_4h, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(candle_backfill_4h, "ensure_env", lambda: None)
    monkeypatch.setattr(candle_backfill_4h, "read_env", lambda: {"BACKFILL_4H_POST_SLEEP": "0"})
    monkeypatch.setattr(candle_backfill_4h, "load_settings", lambda: settings)
    monkeypatch.setattr(candle_backfill_4h, "resolve_feed_symbols", lambda settings: (["BTCUSD"], ["USD"], "test"))
    monkeypatch.setattr(candle_backfill_4h, "SignalMakerClient", FakeSignalMakerClient)
    monkeypatch.setattr(candle_backfill_4h, "fetch_exchange_klines", fake_fetch)
    monkeypatch.setattr(candle_backfill_4h, "_start_ms_for_days", lambda days: 1000)
    monkeypatch.setattr(candle_backfill_4h, "RateLimiter", lambda requests_per_minute: NoSleepLimiter())

    summary = candle_backfill_4h.run_once(days=365, max_symbols=1, max_chunks_per_symbol=1, enabled_override=True)

    assert summary["status"] == "completed"
    assert summary["posted"] == 2
    assert summary["results"][0]["first_remote_open_time"] == 3000
    assert len(posted) == 1
    assert [candle["open_time"] for candle in posted[0]["candles"]] == [1000, 2000]
    assert all(candle["open_time"] < 3000 for candle in posted[0]["candles"])
    assert posted[0]["source"] == "gw-test-backfill-4h-365d"

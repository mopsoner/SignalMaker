import threading
from unittest.mock import Mock

import pytest

from scripts import run_kraken_candle_feed_loop as feed


def test_feed_settings_are_loaded_from_environment():
    settings = feed.FeedSettings.from_env(
        {
            "KRAKEN_CANDLE_FEED_ENABLED": "yes",
            "KRAKEN_CANDLE_FEED_POLL_SECONDS": "45",
            "KRAKEN_CANDLE_FEED_INTERVALS": "15m, 1h",
            "KRAKEN_CANDLE_FEED_QUOTE_ASSETS": "usd, usdc",
            "KRAKEN_CANDLE_FEED_LIMIT": "75",
            "KRAKEN_CANDLE_FEED_MAX_SYMBOLS": "12",
            "KRAKEN_CANDLE_FEED_MARGIN_ONLY": "false",
        }
    )

    assert settings == feed.FeedSettings(True, 45, ["15m", "1h"], ["USD", "USDC"], 75, 12, False)


def test_run_once_passes_feed_settings_to_internal_importer(monkeypatch):
    settings = feed.FeedSettings(True, 30, ["4h"], ["USD"], 42, 5, True)
    db = Mock()
    monkeypatch.setattr(feed, "SessionLocal", Mock(return_value=db))
    importer = Mock(return_value={"status": "ok"})
    monkeypatch.setattr(feed, "import_kraken_candles", importer)

    assert feed.run_once(settings) == {"status": "ok"}
    importer.assert_called_once_with(
        db=db,
        quote_assets=["USD"],
        intervals=["4h"],
        limit=42,
        max_symbols=5,
        margin_only=True,
    )
    db.close.assert_called_once_with()


def test_disabled_feed_does_not_open_database(monkeypatch):
    session_factory = Mock()
    monkeypatch.setattr(feed, "SessionLocal", session_factory)

    result = feed.run_once(feed.FeedSettings(False, 60, ["4h"], ["USD"], 120, 0, True))

    assert result["status"] == "disabled"
    session_factory.assert_not_called()


def test_loop_runs_until_stop_event(monkeypatch):
    stopping = threading.Event()
    run_once = Mock(side_effect=lambda _settings: stopping.set() or {"status": "ok"})
    monkeypatch.setattr(feed, "run_once", run_once)
    settings = feed.FeedSettings(True, 60, ["4h"], ["USD"], 120, 0, True)

    feed.run_loop(settings, stopping)

    run_once.assert_called_once_with(settings)


@pytest.mark.parametrize("name,value", [("KRAKEN_CANDLE_FEED_POLL_SECONDS", "0"), ("KRAKEN_CANDLE_FEED_LIMIT", "many")])
def test_invalid_numeric_setting_is_rejected(name, value):
    with pytest.raises(ValueError):
        feed.FeedSettings.from_env({name: value})

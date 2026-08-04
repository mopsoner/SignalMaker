from unittest.mock import MagicMock
import asyncio

import pytest

from app.api import deps
from scripts import run_scheduler_loop
from app.api.routes import admin_market_data
from app.services.shared_market_analysis_service import SharedMarketAnalysisService


def test_get_db_closes_session_on_success(monkeypatch):
    db = MagicMock()
    monkeypatch.setattr(deps, "SessionLocal", lambda: db)
    dependency = deps.get_db()
    assert next(dependency) is db
    with pytest.raises(StopIteration):
        next(dependency)
    db.close.assert_called_once_with()
    db.rollback.assert_not_called()


def test_get_db_rolls_back_and_closes_on_exception(monkeypatch):
    db = MagicMock()
    monkeypatch.setattr(deps, "SessionLocal", lambda: db)
    dependency = deps.get_db()
    next(dependency)
    with pytest.raises(RuntimeError, match="request failed"):
        dependency.throw(RuntimeError("request failed"))
    db.rollback.assert_called_once_with()
    db.close.assert_called_once_with()


def test_scheduler_uses_fresh_session_for_every_tick(monkeypatch):
    sessions = [MagicMock(name="first"), MagicMock(name="second")]
    factory = MagicMock(side_effect=sessions)
    monkeypatch.setattr(
        run_scheduler_loop,
        "load_runtime_settings",
        lambda db: {"bot": {"bot_scheduler_enabled": False}},
    )

    run_scheduler_loop.run_scheduler_tick(factory)
    run_scheduler_loop.run_scheduler_tick(factory)

    assert factory.call_count == 2
    for session in sessions:
        session.close.assert_called_once_with()


def test_analyze_closes_read_transaction_before_engine_work(monkeypatch):
    events = []
    read_db, write_db = MagicMock(name="read_db"), MagicMock(name="write_db")
    read_db.rollback.side_effect = lambda: events.append("read_ended")
    factory = MagicMock(side_effect=[read_db, write_db])

    class Repo:
        def __init__(self, db):
            self.db = db

        async def list_enabled_market_assets(self, **kwargs):
            return [{"id": "asset-1", "provider_symbol": "MSFT.US"}]

        async def load_stock_etf_candles_for_asset(self, asset_id, timeframe):
            return []

        async def create_analysis_run(self, *args, **kwargs):
            return 42

        async def insert_analysis_result(self, *args, **kwargs):
            events.append("result_written")

        async def finish_analysis_run(self, *args, **kwargs):
            pass

    original_run = SharedMarketAnalysisService.run

    def observed_run(self, **kwargs):
        events.append("analyzed")
        return original_run(self, **kwargs)

    monkeypatch.setattr(admin_market_data, "SessionLocal", factory)
    monkeypatch.setattr(admin_market_data, "MarketDataRepository", Repo)
    monkeypatch.setattr(SharedMarketAnalysisService, "run", observed_run)

    response = asyncio.run(admin_market_data.analyze({"symbols": ["MSFT.US"], "engine": "momentum"}))

    assert response["results"][0]["symbol"] == "MSFT.US"
    assert events.index("read_ended") < events.index("analyzed") < events.index("result_written")
    assert factory.call_count == 2

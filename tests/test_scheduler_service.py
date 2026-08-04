import asyncio
from datetime import datetime, timezone

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.services.scheduler_service import SchedulerService
from signalmaker.market_data.repository import MarketDataRepository


def instant(value):
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def test_exchange_calendar_closed_holiday_and_dst():
    config = {"exchange_timezone": "Europe/Paris", "market_open": "09:00", "market_close": "17:30",
              "exchange_holidays": ["2026-12-25"]}
    assert not SchedulerService.market_is_open(instant("2026-12-25T10:00:00"), config)
    assert not SchedulerService.market_is_open(instant("2026-08-02T10:00:00"), config)  # Sunday
    assert SchedulerService.market_is_open(instant("2026-03-30T07:30:00"), config)  # CEST
    assert SchedulerService.market_is_open(instant("2026-10-26T08:30:00"), config)  # CET


def repository():
    db = Session(create_engine("sqlite://"))
    repo = MarketDataRepository(db); repo.ensure_schema()
    universe = asyncio.run(repo.create_or_update_universe("Europe Stocks", asset_type="STOCK"))
    asset = asyncio.run(repo.upsert_market_asset(universe, "AIR", "AIR.PA", "PA", "Airbus", "STOCK", "EU", "FR", "EUR",
                                                     pea_eligible=False, ucits=False))
    db.execute(text("INSERT INTO stock_etf_candles(asset_id,provider,provider_symbol,timeframe,timestamp,open,high,low,close) VALUES (:a,'IBKR','AIR.PA','1d','2026-08-03 17:30:00',1,2,1,2)"), {"a": asset})
    db.commit()
    return repo


def config():
    return {"enabled": True, "engine": "momentum", "cadence_hours": 24,
            "universes": ["Europe Stocks"], "asset_types": ["STOCK"], "timeframes": ["1d"]}


def test_no_new_candle_does_not_schedule_again_while_market_closed():
    repo = repository()
    service = SchedulerService(repo, now=lambda: instant("2026-08-04T22:00:00"))
    first = asyncio.run(service.schedule_workflow("stock_etf_momentum", config()))
    repo.db.execute(text("UPDATE market_data_job_requests SET status='completed', finished_at='2026-08-03 20:00:00' WHERE id=:id"), {"id": first})
    repo.db.commit()
    assert asyncio.run(service.schedule_workflow("stock_etf_momentum", config())) is None


def test_feeder_targets_only_updated_assets():
    repo = repository(); service = SchedulerService(repo)
    settings = {"stock_etf_momentum": config(), "stock_etf_wyckoff_smc": {"enabled": False}}
    jobs = asyncio.run(service.feeder_completed(settings, ["AIR.PA"]))
    row = asyncio.run(repo.job_requests())[0]
    assert jobs and '"symbols": ["AIR.PA"]' in row["payload"]
    assert '"trigger_cause": "feeder"' in row["payload"]


def test_interrupted_run_is_recovered_but_active_run_is_not_overlapped():
    repo = repository()
    job = asyncio.run(repo.create_job_request("analysis", payload={"workflow": "stock_etf_momentum"}))
    repo.db.execute(text("UPDATE market_data_job_requests SET status='running', attempts=1, heartbeat_at='2020-01-01' WHERE id=:id"), {"id": job})
    recovered = asyncio.run(repo.recover_abandoned_analysis_jobs(timeout_seconds=30))
    assert recovered == 1
    claimed = asyncio.run(repo.claim_next_analysis_job("worker")); repo.db.commit()
    assert claimed["id"] == job
    assert asyncio.run(SchedulerService(repo).schedule_workflow("stock_etf_momentum", config())) is None

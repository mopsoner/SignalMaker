#!/usr/bin/env python3
"""
Scheduler worker — runs lightweight periodic orchestration.
It currently performs live position reconciliation when enabled.
"""
import os
import sys
import time
import logging
import asyncio

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.session import SessionLocal
from app.services.executor_service import ExecutorService
from app.services.runtime_settings import load_runtime_settings
from signalmaker.market_data.repository import MarketDataRepository
from signalmaker.market_data.analysis_service import MarketAnalysisService

DEFAULT_INTERVAL = 30
logger = logging.getLogger(__name__)


async def run_queued_market_analysis(db) -> dict | None:
    """Let the existing scheduler consume analysis work; no dedicated worker."""
    repo = MarketDataRepository(db)
    repo.ensure_schema()
    job = await repo.next_queued_analysis_job()
    if not job:
        return None
    payload = job.get("payload") or {}
    if payload.get("market_scope", "stock_etf") != "stock_etf":
        await repo.update_job_request(job["id"], "IGNORED", result={**payload, "reason": "unsupported_market_scope"})
        db.commit()
        return {"status": "ignored", "job_id": job["id"]}
    await repo.update_job_request(job["id"], "RUNNING", result=payload)
    db.commit()
    try:
        report = await MarketAnalysisService(repo, market_scope="stock_etf").run(
            engine=payload.get("engine", "both"), universe=payload.get("universe"),
            asset_type=payload.get("asset_type"), limit=int(payload.get("limit") or 50),
            timeframe=payload.get("timeframe", "15m"), symbols=payload.get("symbols"),
        )
    except Exception as exc:
        db.rollback()
        await repo.update_job_request(job["id"], "ERROR", result={**payload, "error": str(exc)})
        db.commit()
        return {"status": "error", "job_id": job["id"]}
    await repo.update_job_request(job["id"], "COMPLETED", result={**payload, "analysis_report": report})
    db.commit()
    return report


def run_scheduler_tick(session_factory=SessionLocal) -> int:
    """Run one scheduler tick with a newly-created, bounded session."""
    interval = DEFAULT_INTERVAL
    db = session_factory()
    logger.debug("scheduler DB session opened")
    try:
        runtime = load_runtime_settings(db)
        queued_analysis = asyncio.run(run_queued_market_analysis(db))
        bot = runtime.get('bot', {})
        live_cfg = runtime.get('live', {})
        interval = int(bot.get('bot_scheduler_interval_sec', DEFAULT_INTERVAL))
        if bot.get('bot_scheduler_enabled', True) and live_cfg.get('live_reconcile_enabled', True):
            result = ExecutorService(db).reconcile_live_positions()
            db.commit()
            print(f'Scheduler reconcile tick: {result}', flush=True)
            if queued_analysis:
                print(f'Scheduler market analysis: {queued_analysis}', flush=True)
        elif not bot.get('bot_scheduler_enabled', True):
            interval = 30
            print('Scheduler disabled', flush=True)
        else:
            db.rollback()
            print('Scheduler tick: live reconciliation disabled', flush=True)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
        logger.debug("scheduler DB session closed")
    return interval

if __name__ == "__main__":
    print("Scheduler worker started", flush=True)
    while True:
        try:
            interval = run_scheduler_tick()
        except Exception as exc:
            print(f'Scheduler error: {exc}', flush=True)
            interval = DEFAULT_INTERVAL

        time.sleep(interval)

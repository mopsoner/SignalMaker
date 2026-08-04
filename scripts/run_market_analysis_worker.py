#!/usr/bin/env python3
"""Dedicated, engine-parameterized STOCK/ETF analysis queue consumer."""
import asyncio
import os
import signal

from app.db.session import SessionLocal
from scripts.worker_runtime import heartbeat
from signalmaker.market_data.repository import MarketDataRepository
from signalmaker.market_data.services import MarketAnalysisJobConsumer

WORKER_ID = "stock_etf_analysis"


async def main():
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stopping.set)
    while not stopping.is_set():
        with SessionLocal() as db:
            repo = MarketDataRepository(db)
            repo.ensure_schema()
            heartbeat(WORKER_ID, state="polling", engines=["wyckoff_smc", "momentum"])
            task = asyncio.create_task(MarketAnalysisJobConsumer(repo, worker_id=WORKER_ID).consume_one())
            stop_task = asyncio.create_task(stopping.wait())
            done, _ = await asyncio.wait((task, stop_task), return_when=asyncio.FIRST_COMPLETED)
            if stop_task in done and not task.done():
                task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            stop_task.cancel()
        if not stopping.is_set():
            await asyncio.sleep(max(1, int(os.getenv("MARKET_ANALYSIS_POLL_SECONDS", "5"))))


if __name__ == "__main__":
    asyncio.run(main())

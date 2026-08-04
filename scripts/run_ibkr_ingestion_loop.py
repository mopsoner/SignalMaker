#!/usr/bin/env python3
"""Periodically ingest IBKR daily candles under a stable worker identity."""
import asyncio
import os
import signal

from scripts.worker_runtime import heartbeat
from signalmaker.jobs.ibkr_backfill_daily import main as ingest


async def main():
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stopping.set)
    while not stopping.is_set():
        heartbeat("ibkr_ingestion", state="running")
        await ingest()
        heartbeat("ibkr_ingestion", state="idle")
        try:
            await asyncio.wait_for(stopping.wait(), int(os.getenv("IBKR_INGEST_INTERVAL_SECONDS", "86400")))
        except asyncio.TimeoutError:
            pass


if __name__ == "__main__":
    asyncio.run(main())

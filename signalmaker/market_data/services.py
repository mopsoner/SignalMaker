"""Workers for durable market-data requests."""
import os
import socket
import asyncio

from signalmaker.market_data.analysis_service import MarketAnalysisService


class MarketAnalysisJobConsumer:
    """Consume requests with atomic claiming, bounded retry, and durable diagnostics."""
    MAX_ATTEMPTS = 3

    def __init__(self, repo, *, worker_id=None, service_factory=MarketAnalysisService):
        self.repo = repo
        self.worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}"
        self.service_factory = service_factory

    async def consume_one(self):
        job = await self.repo.claim_next_analysis_job(self.worker_id, max_attempts=self.MAX_ATTEMPTS)
        if not job:
            return None
        self._commit()
        payload = job.get("payload") or {}
        await self.repo.heartbeat_job(job["id"], self.worker_id)
        self._commit()
        try:
            timeout = max(int(payload.get("timeout_seconds") or 1800), 1)
            report = await asyncio.wait_for(self.service_factory(self.repo, market_scope="stock_etf").run(
                engine=payload.get("engine", "both"), universe=payload.get("universe"),
                asset_type=payload.get("asset_type"), limit=int(payload.get("limit") or 50),
                timeframe=(payload.get("timeframes") or [payload.get("timeframe", "15m")])[0],
                symbols=payload.get("symbols"),
            ), timeout=timeout)
            report["worker_id"] = self.worker_id
            terminal = "failed" if report["summary"]["failed"] and not report["summary"]["completed"] else "completed"
            await self.repo.update_job_request(job["id"], terminal, result={**payload, "analysis_report": report})
            self._commit()
            return report
        except Exception as exc:
            self._rollback()
            attempts = int(job.get("attempts") or 1)
            transient = isinstance(exc, (TimeoutError, ConnectionError, OSError))
            status = "queued" if transient and attempts < self.MAX_ATTEMPTS else "failed"
            await self.repo.update_job_request(job["id"], status, result={**payload, "last_error": f"{type(exc).__name__}: {exc}"})
            self._commit()
            return {"status": status, "job_id": job["id"], "last_error": str(exc)}

    def _commit(self): self.repo.db.commit()
    def _rollback(self): self.repo.db.rollback()

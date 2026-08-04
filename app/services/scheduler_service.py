"""Data-aware orchestration for the stock/ETF analysis workflows."""
from __future__ import annotations

import json
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, text


class SchedulerService:
    """Plan durable jobs without re-analysing an unchanged, closed market."""

    def __init__(self, repo=None, *, now=None):
        self.repo = repo
        self._now = now or (lambda: datetime.now(timezone.utc))

    @staticmethod
    def _dt(value):
        if value is None or isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    @staticmethod
    def market_is_open(now: datetime, config: dict) -> bool:
        """Exchange-local session check; ZoneInfo handles both DST transitions."""
        local = now.astimezone(ZoneInfo(config.get("exchange_timezone", "Europe/Paris")))
        holidays = {str(day) for day in config.get("exchange_holidays", [])}
        if local.weekday() >= 5 or local.date().isoformat() in holidays:
            return False
        opened = time.fromisoformat(config.get("market_open", "09:00"))
        closed = time.fromisoformat(config.get("market_close", "17:30"))
        return opened <= local.time().replace(tzinfo=None) < closed

    async def _latest_candle(self, config: dict):
        universes = config.get("universes") or []
        asset_types = config.get("asset_types") or []
        timeframes = config.get("timeframes") or []
        statement = text("""
            SELECT MAX(c.timestamp) AS candle_at
            FROM stock_etf_candles c JOIN market_assets a ON a.id=c.asset_id
            LEFT JOIN market_universes u ON u.id=a.universe_id
            WHERE a.enabled=true
              AND (:all_universes OR u.name IN :universes)
              AND (:all_types OR a.asset_type IN :asset_types)
              AND (:all_timeframes OR c.timeframe IN :timeframes)
        """).bindparams(bindparam("universes", expanding=True), bindparam("asset_types", expanding=True),
                         bindparam("timeframes", expanding=True))
        row = self.repo.db.execute(statement, {
            "all_universes": not universes, "universes": tuple(universes or [""]),
            "all_types": not asset_types, "asset_types": tuple(asset_types or [""]),
            "all_timeframes": not timeframes, "timeframes": tuple(timeframes or [""]),
        }).first()
        return self._dt(row[0]) if row and row[0] else None

    async def _last_job(self, workflow: str):
        rows = await self.repo.job_requests(limit=200)
        for row in rows:
            payload = row.get("payload") or {}
            if isinstance(payload, str):
                payload = json.loads(payload or "{}")
            if row.get("job_type") == "analysis" and payload.get("workflow") == workflow:
                return row, payload
        return None, {}

    async def schedule_workflow(self, workflow: str, config: dict, *, cause="reconciliation", symbols=None):
        if not config.get("enabled", True):
            return None
        now = self._now()
        latest = await self._latest_candle(config)
        last, last_payload = await self._last_job(workflow)
        if last and str(last.get("status", "")).lower() in {"queued", "running"}:
            return None                         # overlap protection
        last_candle = self._dt(last_payload.get("last_closed_candle_at"))
        cadence = timedelta(hours=float(config.get("cadence_hours", 24)))
        last_finished = self._dt(last.get("finished_at")) if last else None
        changed = latest is not None and (last_candle is None or latest > last_candle)
        due = not last_finished or now - last_finished.replace(tzinfo=last_finished.tzinfo or timezone.utc) >= cadence
        # Feeder events are targeted and immediate; reconciliation requires cadence.
        if not changed or (cause == "reconciliation" and not due):
            return None
        payload = {
            "workflow": workflow, "engine": config.get("engine", workflow),
            "universes": config.get("universes", []), "asset_types": config.get("asset_types", []),
            "timeframes": config.get("timeframes", ["1d"]), "symbols": symbols,
            "trigger_cause": cause, "last_closed_candle_at": latest.isoformat(),
            "timeout_seconds": int(config.get("timeout_seconds", 1800)),
        }
        return await self.repo.create_job_request("analysis", payload=payload)

    async def feeder_completed(self, settings: dict, symbols: list[str]) -> list:
        """Queue analysis only for symbols for which a feeder stored new bars."""
        jobs = []
        for workflow, config in self._workflow_configs(settings).items():
            job = await self.schedule_workflow(workflow, config, cause="feeder", symbols=symbols)
            if job is not None:
                jobs.append(job)
        return jobs

    async def tick(self, settings: dict):
        timeout = max(int(settings.get("scheduler", {}).get("abandoned_after_seconds", 900)), 1)
        recovered = await self.repo.recover_abandoned_analysis_jobs(timeout_seconds=timeout)
        queued = []
        for workflow, config in self._workflow_configs(settings).items():
            job = await self.schedule_workflow(workflow, config)
            if job is not None:
                queued.append(job)
        return {"queued": queued, "recovered": recovered}

    @staticmethod
    def _workflow_configs(settings: dict) -> dict[str, dict]:
        stock = settings.get("stock_etf")
        if not isinstance(stock, dict):
            return {name: settings.get(name, {}) for name in ("stock_etf_wyckoff_smc", "stock_etf_momentum")}
        common = {key: stock.get(key) for key in ("universes", "asset_types", "timeframes", "exchange_timezone", "market_open", "market_close", "timeout_seconds")}
        return {
            "stock_etf_wyckoff_smc": {**common, "engine": "wyckoff_smc", "enabled": stock.get("wyckoff_smc_enabled", False), "cadence_hours": stock.get("wyckoff_smc_cadence_hours", 1)},
            "stock_etf_momentum": {**common, "engine": "momentum", "enabled": stock.get("momentum_enabled", False), "cadence_hours": stock.get("momentum_cadence_hours", 24)},
        }

    async def status(self, settings: dict | None = None) -> dict:
        now = self._now()
        rows = await self.repo.job_requests(limit=200) if self.repo else []
        jobs = []
        workflow_configs = self._workflow_configs(settings or {})
        for row in rows:
            payload = row.get("payload") or {}
            if isinstance(payload, str): payload = json.loads(payload or "{}")
            if row.get("job_type") != "analysis": continue
            workflow = payload.get("workflow")
            cadence = float(workflow_configs.get(workflow, {}).get("cadence_hours", 0))
            finished = self._dt(row.get("finished_at"))
            next_run = finished + timedelta(hours=cadence) if finished and cadence else None
            jobs.append({"workflow": workflow, "status": row.get("status"),
                         "last_run": row.get("started_at"), "trigger_cause": payload.get("trigger_cause"),
                         "next_run": next_run.isoformat() if next_run else None,
                         "universes": payload.get("universes", []),
                         "assets_remaining": payload.get("assets_remaining", len(payload.get("symbols") or []))})
        return {"service": "scheduler", "status": "ready", "heartbeat_at": now.isoformat(), "jobs": jobs}

    def heartbeat(self) -> dict:
        return {"service": "scheduler", "status": "ready", "heartbeat_at": self._now().isoformat()}

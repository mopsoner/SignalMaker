"""Application orchestration for every market analysis entry point."""
from __future__ import annotations

import hashlib
import json
import uuid
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from app.services.momentum_service import MomentumService
from app.services.shared_market_analysis_service import SharedMarketAnalysisService as SharedEngineService
from signalmaker.market_data.analysis_adapter import MarketAnalysisAdapter


class MarketAnalysisService:
    """Run the shared crypto workflows against a scoped market candle source.

    This is deliberately the only layer which orchestrates stock/ETF analysis.
    The adapter is a data/normalisation boundary; workflow selection, run
    persistence, failure isolation, and idempotency belong here.
    """

    WORKFLOW_VERSION = "stock-etf-shared-v1"
    ENGINES = ("momentum", "wyckoff_smc")
    SUPPORTED_EXECUTION_TIMEFRAMES = ("15m", "1h", "4h")

    @classmethod
    def validate_request(cls, engine: str, timeframe: str) -> None:
        """Reject legacy requests instead of silently changing their meaning."""
        if engine not in {*cls.ENGINES, "both"}:
            raise ValueError(f"unsupported_engine:{engine}")
        if timeframe not in cls.SUPPORTED_EXECUTION_TIMEFRAMES:
            raise ValueError(
                f"legacy_analysis_timeframe:{timeframe}; migration required: "
                "use 15m, 1h, or 4h with complete 15m/1h/4h candle history"
            )

    def __init__(self, repo, *, adapter=None, pipeline=None, market_scope: str = "stock_etf"):
        if market_scope != "stock_etf":
            raise ValueError(f"unsupported_market_scope:{market_scope}")
        self.repo = repo
        self.adapter = adapter or MarketAnalysisAdapter(repo)
        self.pipeline = pipeline or SharedEngineService()
        self.market_scope = market_scope

    @classmethod
    def required_timeframes(cls, engine: str, requested: str = "15m") -> tuple[str, ...]:
        cls.validate_request(engine, requested)
        if engine == "momentum":
            return tuple(MomentumService.INTERVALS)
        return tuple(dict.fromkeys((requested, "1h", "4h")))

    async def _idempotency_key(self, asset: dict, engine: str, timeframe: str) -> str:
        timeframes = self.required_timeframes(engine, timeframe)
        bundle = await self.adapter.load_stock_etf_candle_bundle(asset["id"], timeframes)
        closed = []
        for tf in timeframes:
            candles = self.adapter.normalize_engine_candles(
                bundle.get(tf, []), symbol=asset.get("provider_symbol") or str(asset["id"]), timeframe=tf
            )
            if candles:
                closed.append((tf, candles[-1]["close_time"]))
        identity = {
            "market_scope": self.market_scope, "asset_id": str(asset["id"]), "engine": engine,
            "last_closed_candles": closed, "timeframes": timeframes,
            "workflow_version": self.WORKFLOW_VERSION,
        }
        return hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()

    async def run(
        self, *, engine: str = "both", universe: str | None = None,
        asset_type: str | None = None, limit: int = 50, timeframe: str = "15m",
        symbols: list[str] | None = None, filters: dict[str, Any] | None = None,
        assets: list[dict] | None = None,
    ) -> dict[str, Any]:
        self.validate_request(engine, timeframe)
        engines = list(self.ENGINES) if engine == "both" else [engine]
        run_token = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc).isoformat()
        metadata = {
            "run_identifier": run_token, "market_scope": self.market_scope,
            "workflow_version": self.WORKFLOW_VERSION, "universe": universe,
            "asset_type": asset_type, "required_timeframes": {
                name: self.required_timeframes(name, timeframe) for name in engines
            },
        }
        run_id = await self.repo.create_analysis_run(engine, timeframe=timeframe, metadata=metadata)
        self._commit()
        if assets is None:
            assets = await self.repo.list_enabled_market_assets(
                universe_name=universe, asset_type=asset_type, limit=limit,
                symbols=symbols, **(filters or {}),
            )

        counts: Counter[str] = Counter()
        results: list[dict] = []
        for asset in assets:
            for selected_engine in engines:
                symbol = asset.get("provider_symbol") or str(asset["id"])
                try:
                    key = await self._idempotency_key(asset, selected_engine, timeframe)
                    if hasattr(self.repo, "analysis_result_exists") and await self.repo.analysis_result_exists(key):
                        counts["skipped"] += 1
                        results.append({"asset_id": asset["id"], "symbol": symbol, "engine_name": selected_engine,
                                        "status": "skipped", "phase": "idempotency_check", "missing_timeframes": [],
                                        "status_history": ["queued", "running", "skipped"]})
                        continue
                    required = self.required_timeframes(selected_engine, timeframe)
                    raw = await self.adapter.load_stock_etf_candle_bundle(asset["id"], required)
                    bundle = {tf: self.adapter.normalize_engine_candles(raw.get(tf, []), symbol=symbol, timeframe=tf) for tf in required}
                    result = self.pipeline.run(
                        market_scope="stock_etf", asset_id=str(asset["id"]), engine=selected_engine,
                        universe=universe or asset.get("universe_name"), asset_type=asset_type or asset.get("asset_type"),
                        timeframes=required, workflow_version=self.WORKFLOW_VERSION, candles=bundle,
                        symbol=symbol, market_context=asset,
                    )
                    status = result["status"]
                    result.update({
                        "market_scope": self.market_scope, "asset_id": str(asset["id"]),
                        "workflow_version": self.WORKFLOW_VERSION, "idempotency_key": key,
                    })
                    try:
                        await self.repo.insert_analysis_result(
                            run_id, asset["id"], selected_engine, timeframe, result,
                            idempotency_key=key, workflow_version=self.WORKFLOW_VERSION,
                        )
                    except TypeError:  # compatibility for external repository implementations
                        await self.repo.insert_analysis_result(run_id, asset["id"], selected_engine, timeframe, result)
                    self._commit()
                    counts[status] += 1
                    results.append({"asset_id": asset["id"], "symbol": symbol,
                                    "status_history": ["queued", "running", status], **result})
                except Exception as exc:  # one bad instrument must not abort its universe
                    self._rollback()
                    counts["failed"] += 1
                    results.append({"asset_id": asset.get("id"), "symbol": symbol, "engine_name": selected_engine,
                                    "status": "failed", "phase": "analyzing", "missing_timeframes": [],
                                    "status_history": ["queued", "running", "failed"],
                                    "error": f"{type(exc).__name__}: {exc}"})

        total = len(assets) * len(engines)
        status = "SUCCESS" if not counts["failed"] else "PARTIAL"
        processed = sum(counts[name] for name in ("completed", "insufficient_data", "skipped", "failed"))
        await self.repo.finish_analysis_run(run_id, status, total, processed, counts["failed"])
        self._commit()
        summary = {"total": total, "processed": processed, **{name: counts[name] for name in ("completed", "insufficient_data", "skipped", "failed")}}
        finished_at = datetime.now(timezone.utc).isoformat()
        last_error = next((row.get("error") for row in reversed(results) if row.get("error")), None)
        return {"run_id": run_id, "run_identifier": run_token, "status": status, "summary": summary,
                **summary, "started_at": started_at, "heartbeat_at": finished_at, "finished_at": finished_at,
                "worker_id": None, "last_error": last_error, "results": results}

    def _commit(self):
        db = getattr(self.repo, "db", None)
        if db is not None:
            db.commit()

    def _rollback(self):
        db = getattr(self.repo, "db", None)
        if db is not None:
            db.rollback()


SharedMarketAnalysisService = MarketAnalysisService

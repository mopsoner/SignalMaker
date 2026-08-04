"""Application orchestration for every market analysis entry point."""
from __future__ import annotations

import hashlib
import json
import uuid
from collections import Counter
from typing import Any

from app.services.momentum_service import MomentumService
from signalmaker.market_data.analysis_adapter import MarketAnalysisAdapter


class MarketAnalysisService:
    """Run the shared crypto workflows against a scoped market candle source.

    This is deliberately the only layer which orchestrates stock/ETF analysis.
    The adapter is a data/normalisation boundary; workflow selection, run
    persistence, failure isolation, and idempotency belong here.
    """

    WORKFLOW_VERSION = "stock-etf-shared-v1"
    ENGINES = ("momentum", "wyckoff_smc")

    def __init__(self, repo, *, adapter=None, market_scope: str = "stock_etf"):
        if market_scope != "stock_etf":
            raise ValueError(f"unsupported_market_scope:{market_scope}")
        self.repo = repo
        self.adapter = adapter or MarketAnalysisAdapter(repo)
        self.market_scope = market_scope

    @classmethod
    def required_timeframes(cls, engine: str, requested: str = "15m") -> tuple[str, ...]:
        if engine == "momentum":
            return tuple(MomentumService.INTERVALS)
        execution = "15m" if requested in {"1d", "5m", "15m"} else requested
        return tuple(dict.fromkeys((execution, "1h", "4h")))

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
            "asset_id": str(asset["id"]), "engine": engine,
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
        if engine not in {*self.ENGINES, "both"}:
            raise ValueError(f"unsupported_engine:{engine}")
        engines = list(self.ENGINES) if engine == "both" else [engine]
        run_token = str(uuid.uuid4())
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
                        results.append({"asset_id": asset["id"], "symbol": symbol, "engine_name": selected_engine, "status": "skipped"})
                        continue
                    result = await (
                        self.adapter.run_momentum_analysis(asset["id"], timeframe)
                        if selected_engine == "momentum"
                        else self.adapter.run_wyckoff_smc_analysis(asset["id"], timeframe, asset=asset)
                    )
                    status = "insufficient_data" if result.get("status") == "insufficient_data" else "analyzed"
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
                    results.append({"asset_id": asset["id"], "symbol": symbol, "status": status, **result})
                except Exception as exc:  # one bad instrument must not abort its universe
                    self._rollback()
                    counts["error"] += 1
                    results.append({"asset_id": asset.get("id"), "symbol": symbol, "engine_name": selected_engine, "status": "error", "error": str(exc)})

        total = len(assets) * len(engines)
        status = "SUCCESS" if not counts["error"] else "PARTIAL"
        await self.repo.finish_analysis_run(run_id, status, total, counts["analyzed"] + counts["insufficient_data"], counts["error"])
        self._commit()
        summary = {name: counts[name] for name in ("analyzed", "insufficient_data", "skipped", "error")}
        return {"run_id": run_id, "run_identifier": run_token, "status": status, "summary": summary, "results": results}

    def _commit(self):
        db = getattr(self.repo, "db", None)
        if db is not None:
            db.commit()

    def _rollback(self):
        db = getattr(self.repo, "db", None)
        if db is not None:
            db.rollback()


SharedMarketAnalysisService = MarketAnalysisService

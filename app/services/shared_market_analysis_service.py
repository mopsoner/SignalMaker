"""Market-neutral entry point for the analysis workflows used by crypto."""
from __future__ import annotations

from typing import Any

from app.services.momentum_service import MomentumService
from app.services.wyckoff_pipeline_service import WyckoffPipelineService


class SharedMarketAnalysisService:
    """Execute the canonical engines against an already-normalized candle bundle."""

    def run(self, *, market_scope: str, asset_id: str, engine: str, universe: str | None,
            asset_type: str | None, timeframes: tuple[str, ...], workflow_version: str,
            candles: dict[str, list[dict[str, Any]]], symbol: str,
            market_context: dict[str, Any] | None = None) -> dict[str, Any]:
        context = {
            "market_scope": market_scope, "asset_id": str(asset_id), "engine": engine,
            "universe": universe, "asset_type": asset_type,
            "required_timeframes": list(timeframes), "workflow_version": workflow_version,
        }
        if engine == "momentum":
            minimums = MomentumService.LOOKBACKS
            missing = [tf for tf in timeframes if len(candles.get(tf, ())) < minimums[tf]]
            if missing:
                return self._insufficient(context, candles, minimums, missing)
            row = MomentumService.calculate_bundle(symbol, candles)
            score = row["momentum_score"]
            return {**context, "status": "completed", "phase": "persisting", "engine_name": engine,
                    "signal": "BUY" if score >= 10 else "SELL" if score < -10 else "HOLD",
                    "score": score, "trend": row["classification"],
                    "confidence": min(1.0, abs(score) / 25), "payload": {**row, **context}}

        minimums = {tf: 24 for tf in timeframes}
        missing = [tf for tf in timeframes if len(candles.get(tf, ())) < minimums[tf]]
        if missing:
            return self._insufficient(context, candles, minimums, missing)
        state, _ = WyckoffPipelineService().analyze(
            symbol=symbol, candles=candles, market_context={**(market_context or {}), **context},
            execution_interval=timeframes[0],
        )
        bias = str(state.get("bias") or "neutral")
        return {**context, "status": "completed", "phase": "persisting", "engine_name": engine,
                "signal": "BUY" if bias.startswith("bull") else "SELL" if bias.startswith("bear") else "HOLD",
                "score": state.get("score"), "trend": state.get("state"),
                "confidence": state.get("confidence"), "stage": state.get("stage"), "payload": state}

    @staticmethod
    def _insufficient(context, candles, minimums, missing):
        return {**context, "status": "insufficient_data", "phase": "validating",
                "engine_name": context["engine"], "signal": "NO_SIGNAL", "score": None,
                "trend": None, "confidence": None, "missing_timeframes": missing,
                "error": "insufficient closed-candle history for: " + ", ".join(missing),
                "payload": {**context, "reason": "INSUFFICIENT_TIMEFRAME_HISTORY",
                            "missing_timeframes": missing,
                            "candle_counts": {tf: len(candles.get(tf, ())) for tf in minimums},
                            "minimum_candles_by_timeframe": minimums}}

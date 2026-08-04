from __future__ import annotations

from typing import Any

from signalmaker.market_data.repository import MarketDataRepository
from app.services.wyckoff_pipeline_service import WyckoffPipelineService
from app.services.momentum_service import MomentumService


class MarketAnalysisAdapter:
    def __init__(self, repo: MarketDataRepository):
        self.repo = repo

    async def load_stock_etf_candles_for_asset(self, asset_id, timeframe="1d"):
        return await self.repo.load_stock_etf_candles_for_asset(asset_id, timeframe)

    async def load_stock_etf_candle_bundle(self, asset_id, timeframes=("15m", "1h", "4h")):
        if hasattr(self.repo, "load_stock_etf_candle_bundle"):
            return await self.repo.load_stock_etf_candle_bundle(asset_id, timeframes)
        return {tf: await self.load_stock_etf_candles_for_asset(asset_id, tf) for tf in timeframes}

    def to_engine_input(self, candles):
        normalized = []
        for candle in candles:
            timestamp = candle.get("timestamp") or candle.get("open_time")
            if hasattr(timestamp, "timestamp"):
                timestamp = int(timestamp.timestamp() * 1000)
            normalized.append({
                **candle, "timestamp": timestamp, "open_time": candle.get("open_time") or timestamp,
                "close_time": candle.get("close_time") or timestamp,
                "open": float(candle["open"]), "high": float(candle["high"]),
                "low": float(candle["low"]), "close": float(candle.get("adjusted_close") or candle["close"]),
                "raw_close": float(candle["close"]), "volume": float(candle.get("volume") or 0),
            })
        return normalized

    async def run_momentum_analysis(self, asset_id, timeframe="1d"):
        timeframes = MomentumService.INTERVALS
        raw_bundle = await self.load_stock_etf_candle_bundle(asset_id, timeframes)
        bundle = {tf: self.to_engine_input(raw_bundle.get(tf, [])) for tf in timeframes}
        row = MomentumService.calculate_bundle(str(asset_id), bundle)
        available = [tf for tf in timeframes if row[f"momentum_{tf}"] is not None]
        timeframe_metadata = {
            "market_type": "stock_etf", "requested_timeframe": timeframe,
            "timeframe_mapping": {tf: (tf if tf in available else None) for tf in timeframes},
            "unavailable_timeframes": [tf for tf in timeframes if tf not in available],
        }
        if not available:
            result = self._no_signal("momentum", 0, 2)
            result["payload"].update({**row, **timeframe_metadata})
            return result
        signal = "NO_SIGNAL" if not available else "BUY" if row["momentum_score"] >= 10 else "SELL" if row["momentum_score"] < -10 else "HOLD"
        return {
            "engine_name": "momentum", "signal": signal, "score": row["momentum_score"] if available else None,
            "trend": row["classification"] if available else None,
            "confidence": min(1.0, abs(row["momentum_score"]) / 25) if available else None,
            "payload": {**row, **timeframe_metadata},
        }

    async def run_wyckoff_smc_analysis(self, asset_id, timeframe="15m", *, asset: dict | None = None):
        execution_timeframe = "15m" if timeframe in {"1d", "5m", "15m"} else timeframe
        timeframes = tuple(dict.fromkeys((execution_timeframe, "1h", "4h")))
        raw_bundle = await self.load_stock_etf_candle_bundle(asset_id, timeframes)
        bundle = {tf: self.to_engine_input(rows) for tf, rows in raw_bundle.items()}
        identity = dict(asset or {})
        context = {
            key: identity.get(key)
            for key in ("asset_id", "provider_symbol", "universe_name", "asset_type", "currency", "exchange_code")
        }
        context["asset_id"] = context.get("asset_id") or identity.get("id") or asset_id
        symbol = identity.get("provider_symbol") or identity.get("symbol") or str(asset_id)
        state, _assessment = WyckoffPipelineService().analyze(
            symbol=symbol, candles=bundle, market_context=context, execution_interval=execution_timeframe
        )
        bias = str(state.get("bias") or "neutral")
        decision = "BUY" if bias.startswith("bull") else "SELL" if bias.startswith("bear") else "HOLD"
        return {
            "engine_name": "wyckoff_smc", "signal": decision, "score": state.get("score"),
            "trend": state.get("state"), "confidence": state.get("confidence"),
            "stage": state.get("stage"), "bias": state.get("bias"),
            "state_payload": state, "payload": state,
            **{key: state.get(key) for key in (
                "hierarchy_gate", "wyckoff_requirement", "one_hour_decision", "confirmation_model",
                "execution_trigger", "liquidity_context", "macro_liquidity_context",
                "entry_liquidity_context", "projected_target", "execution_target")},
        }

    def _no_signal(self, engine, count, minimum):
        return {"engine_name": engine, "signal": "NO_SIGNAL", "score": None, "trend": None, "confidence": None, "payload": {"reason": "NOT_ENOUGH_CANDLES", "candles_count": count, "minimum_candles": minimum}}

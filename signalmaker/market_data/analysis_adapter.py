from __future__ import annotations

from statistics import fmean
from typing import Any

from signalmaker.data_providers.eodhd.repository import EODHDRepository


class MarketAnalysisAdapter:
    def __init__(self, repo: EODHDRepository):
        self.repo = repo

    async def load_candles_for_asset(self, asset_id, timeframe="1d"):
        return await self.repo.load_candles_for_asset(asset_id, timeframe)

    def to_engine_input(self, candles):
        return [{
            "timestamp": c["timestamp"], "open": float(c["open"]), "high": float(c["high"]),
            "low": float(c["low"]), "close": float(c.get("adjusted_close") or c["close"]),
            "raw_close": float(c["close"]), "volume": float(c.get("volume") or 0),
        } for c in candles]

    async def run_momentum_analysis(self, asset_id, timeframe="1d"):
        candles = self.to_engine_input(await self.load_candles_for_asset(asset_id, timeframe))
        if len(candles) < 200:
            return self._no_signal("momentum", len(candles), 200)
        closes = [c["close"] for c in candles]
        ma50 = fmean(closes[-50:]); ma200 = fmean(closes[-200:]); last = closes[-1]
        ret_20 = (last / closes[-21] - 1) * 100 if closes[-21] else 0
        score = round((last / ma200 - 1) * 100 + ret_20, 4)
        signal = "BUY" if last > ma50 > ma200 and score > 0 else "SELL" if last < ma200 else "HOLD"
        return {"engine_name": "momentum", "signal": signal, "score": score, "trend": "UP" if last > ma200 else "DOWN", "confidence": min(1.0, abs(score) / 25), "payload": {"ma50": ma50, "ma200": ma200, "return_20d_pct": ret_20, "candles_count": len(candles)}}

    async def run_wyckoff_smc_analysis(self, asset_id, timeframe="1d"):
        candles = self.to_engine_input(await self.load_candles_for_asset(asset_id, timeframe))
        if len(candles) < 100:
            return self._no_signal("wyckoff_smc", len(candles), 100)
        highs = [c["high"] for c in candles[-50:]]; lows = [c["low"] for c in candles[-50:]]; close = candles[-1]["close"]
        rng_high = max(highs[:-1]); rng_low = min(lows[:-1])
        signal = "BUY" if close > rng_high else "SELL" if close < rng_low else "HOLD"
        trend = "ACCUMULATION_BREAKOUT" if signal == "BUY" else "DISTRIBUTION_BREAKDOWN" if signal == "SELL" else "RANGE"
        score = 100 * (close - rng_low) / (rng_high - rng_low) if rng_high != rng_low else 50
        return {"engine_name": "wyckoff_smc", "signal": signal, "score": round(score, 4), "trend": trend, "confidence": 0.65 if signal != "HOLD" else 0.35, "payload": {"range_high": rng_high, "range_low": rng_low, "candles_count": len(candles)}}

    def _no_signal(self, engine, count, minimum):
        return {"engine_name": engine, "signal": "NO_SIGNAL", "score": None, "trend": None, "confidence": None, "payload": {"reason": "NOT_ENOUGH_CANDLES", "candles_count": count, "minimum_candles": minimum}}

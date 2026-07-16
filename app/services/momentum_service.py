from __future__ import annotations

from datetime import datetime, timezone
from statistics import fmean
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.momentum_current import MomentumCurrent
from app.models.momentum_structure_current import MomentumStructureCurrent
from app.services.market_data_service import MarketDataService


class MomentumService:
    """Momentum calculation and snapshot service.

    Heavy calculations happen explicitly after candle ingestion through
    recalculate_and_store(). Read paths use the persisted momentum_current table.
    """

    INTERVALS = ("15m", "1h", "4h")
    WEIGHTS = {"15m": 0.35, "1h": 0.40, "4h": 0.25}
    LOOKBACKS = {"15m": 32, "1h": 24, "4h": 30}
    EMA_PERIOD = 20
    RSI_PERIOD = 14
    STRUCTURE_SWING_WINDOW = 2
    ACCELERATION_WEIGHT = 0.30
    ACCELERATION_CAP = 30.0
    ACCELERATION_WEIGHTS = {"15m": 0.20, "1h": 0.45, "4h": 0.35}

    def __init__(self, db: Session) -> None:
        self.db = db
        self.market_data = MarketDataService(db)

    def list_rankings(self, *, limit: int = 200) -> list[dict[str, Any]]:
        stmt = select(MomentumCurrent).order_by(MomentumCurrent.momentum_score.desc(), MomentumCurrent.symbol.asc()).limit(limit)
        rows = list(self.db.scalars(stmt).all())
        structures = self._structure_map([row.symbol for row in rows])
        return [self._row_to_payload(row, index, structures.get(row.symbol)) for index, row in enumerate(rows, start=1)]

    def recalculate_and_store(self, *, symbols: list[str] | None = None, limit: int | None = None) -> dict[str, Any]:
        target_symbols = sorted({symbol.upper() for symbol in (symbols or self.market_data.list_symbols(limit=None))})
        if limit is not None:
            target_symbols = target_symbols[:limit]

        previous_rows = {
            row.symbol: row
            for row in self.db.scalars(
                select(MomentumCurrent).where(MomentumCurrent.symbol.in_(target_symbols))
            ).all()
        } if target_symbols else {}

        calculated_rows = [self._build_symbol_row(symbol, previous=previous_rows.get(symbol)) for symbol in target_symbols]
        calculated_rows.sort(key=lambda row: row["momentum_score"], reverse=True)
        now = datetime.now(timezone.utc)
        for index, payload in enumerate(calculated_rows, start=1):
            payload["rank"] = index
            self._upsert_current(payload, calculated_at=now)
            self._upsert_structure(payload["symbol"], payload["structure_15m"], calculated_at=now)
        self.db.commit()
        updated = [
            {
                "symbol": row["symbol"],
                "updated_timeframes": row["updated_timeframes"],
                "momentum_acceleration": row["momentum_acceleration"],
            }
            for row in calculated_rows
            if row["updated_timeframes"]
        ]
        return {
            "symbols": len(target_symbols),
            "momentum_rows_upserted": len(calculated_rows),
            "calculated_at": now.isoformat(),
            "updated": updated,
        }

    def _structure_map(self, symbols: list[str]) -> dict[str, MomentumStructureCurrent]:
        if not symbols:
            return {}
        rows = self.db.scalars(select(MomentumStructureCurrent).where(MomentumStructureCurrent.symbol.in_(symbols))).all()
        return {row.symbol: row for row in rows}

    def _row_to_payload(self, row: MomentumCurrent, rank: int, structure: MomentumStructureCurrent | None = None) -> dict[str, Any]:
        payload = {
            "rank": rank,
            "symbol": row.symbol,
            "price": row.price,
            "momentum_15m": row.momentum_15m,
            "momentum_1h": row.momentum_1h,
            "momentum_4h": row.momentum_4h,
            "momentum_score": row.momentum_score,
            "momentum_delta_15m": row.momentum_delta_15m,
            "momentum_delta_1h": row.momentum_delta_1h,
            "momentum_delta_4h": row.momentum_delta_4h,
            "momentum_acceleration_15m": row.momentum_acceleration_15m,
            "momentum_acceleration_1h": row.momentum_acceleration_1h,
            "momentum_acceleration_4h": row.momentum_acceleration_4h,
            "momentum_acceleration": row.momentum_acceleration,
            "momentum_candle_time_15m": row.momentum_candle_time_15m,
            "momentum_candle_time_1h": row.momentum_candle_time_1h,
            "momentum_candle_time_4h": row.momentum_candle_time_4h,
            "classification": row.classification,
            "rsi_15m": row.rsi_15m,
            "rsi_1h": row.rsi_1h,
            "rsi_4h": row.rsi_4h,
            "change_15m": row.change_15m,
            "change_1h": row.change_1h,
            "change_4h": row.change_4h,
            "ema_trend_15m": row.ema_trend_15m,
            "ema_trend_1h": row.ema_trend_1h,
            "ema_trend_4h": row.ema_trend_4h,
            "updated_at": row.updated_at,
            "data_quality": row.data_quality,
            "calculated_at": row.calculated_at,
        }
        payload.update(self._structure_payload(structure))
        return payload

    def _structure_payload(self, row: MomentumStructureCurrent | None) -> dict[str, Any]:
        if row is None:
            return {
                "structure_15m_status": "unknown",
                "structure_15m_bias": "neutral",
                "mss_15m_bearish": False,
                "bos_15m_bearish": False,
                "bos_15m_bullish": False,
                "last_swing_low_15m": None,
                "last_swing_high_15m": None,
                "structure_broken_at": None,
                "structure_reason": "structure_not_calculated",
            }
        return {
            "structure_15m_status": row.structure_15m_status,
            "structure_15m_bias": row.structure_15m_bias,
            "mss_15m_bearish": row.mss_15m_bearish,
            "bos_15m_bearish": row.bos_15m_bearish,
            "bos_15m_bullish": row.bos_15m_bullish,
            "last_swing_low_15m": row.last_swing_low_15m,
            "last_swing_high_15m": row.last_swing_high_15m,
            "structure_broken_at": row.structure_broken_at,
            "structure_reason": row.structure_reason,
        }

    def _upsert_current(self, payload: dict[str, Any], *, calculated_at: datetime) -> None:
        row = self.db.get(MomentumCurrent, payload["symbol"])
        if row is None:
            row = MomentumCurrent(symbol=payload["symbol"])
            self.db.add(row)
        for key in (
            "price", "momentum_15m", "momentum_1h", "momentum_4h", "momentum_score",
            "momentum_delta_15m", "momentum_delta_1h", "momentum_delta_4h",
            "momentum_acceleration_15m", "momentum_acceleration_1h", "momentum_acceleration_4h",
            "momentum_acceleration", "momentum_candle_time_15m", "momentum_candle_time_1h",
            "momentum_candle_time_4h", "classification",
            "rsi_15m", "rsi_1h", "rsi_4h", "change_15m", "change_1h", "change_4h",
            "ema_trend_15m", "ema_trend_1h", "ema_trend_4h", "data_quality", "rank", "updated_at",
        ):
            setattr(row, key, payload.get(key))
        row.calculated_at = calculated_at

    def _upsert_structure(self, symbol: str, payload: dict[str, Any], *, calculated_at: datetime) -> None:
        row = self.db.get(MomentumStructureCurrent, symbol)
        if row is None:
            row = MomentumStructureCurrent(symbol=symbol)
            self.db.add(row)
        row.structure_15m_status = payload["structure_15m_status"]
        row.structure_15m_bias = payload["structure_15m_bias"]
        row.mss_15m_bearish = payload["mss_15m_bearish"]
        row.bos_15m_bearish = payload["bos_15m_bearish"]
        row.bos_15m_bullish = payload["bos_15m_bullish"]
        row.last_swing_low_15m = payload["last_swing_low_15m"]
        row.last_swing_high_15m = payload["last_swing_high_15m"]
        row.structure_broken_at = payload["structure_broken_at"]
        row.structure_reason = payload["structure_reason"]
        row.calculated_at = calculated_at

    def _build_symbol_row(self, symbol: str, previous: MomentumCurrent | None = None) -> dict[str, Any]:
        bundle = self.market_data.load_symbol_bundle(symbol, {interval: self.LOOKBACKS[interval] for interval in self.INTERVALS})
        interval_payloads = {interval: self._interval_momentum(bundle.get(interval) or []) for interval in self.INTERVALS}
        structure_15m = self._structure_15m(bundle.get("15m") or [])

        weighted_score = 0.0
        available_weight = 0.0
        for interval, payload in interval_payloads.items():
            value = payload["momentum"]
            if value is None:
                continue
            weight = self.WEIGHTS[interval]
            weighted_score += value * weight
            available_weight += weight
        raw_momentum_score = weighted_score / available_weight if available_weight else 0.0

        momentum_deltas: dict[str, float] = {}
        momentum_accelerations: dict[str, float] = {}
        momentum_candle_times: dict[str, datetime | None] = {}
        updated_timeframes: list[str] = []
        for interval in self.INTERVALS:
            current_value = interval_payloads[interval]["momentum"]
            current_candle_time = interval_payloads[interval]["candle_time"]
            previous_candle_time = getattr(previous, f"momentum_candle_time_{interval}") if previous else None
            has_new_candle = (
                current_candle_time is not None
                and (previous_candle_time is None or current_candle_time > previous_candle_time)
            )

            if previous is None:
                current_delta = 0.0
                current_acceleration = 0.0
                candle_time = current_candle_time
                if current_candle_time is not None:
                    updated_timeframes.append(interval)
            elif not has_new_candle:
                current_delta = float(getattr(previous, f"momentum_delta_{interval}") or 0.0)
                current_acceleration = float(getattr(previous, f"momentum_acceleration_{interval}") or 0.0)
                candle_time = previous_candle_time
            else:
                previous_momentum = getattr(previous, f"momentum_{interval}")
                previous_delta = getattr(previous, f"momentum_delta_{interval}")
                current_delta = self._safe_delta(current_value, previous_momentum)
                current_acceleration = self._safe_acceleration(current_delta, previous_delta)
                candle_time = current_candle_time
                updated_timeframes.append(interval)

            momentum_deltas[interval] = current_delta
            momentum_accelerations[interval] = current_acceleration
            momentum_candle_times[interval] = candle_time

        acceleration_weighted_sum = 0.0
        acceleration_available_weight = 0.0
        for interval, weight in self.ACCELERATION_WEIGHTS.items():
            if interval_payloads[interval]["momentum"] is None:
                continue
            acceleration_weighted_sum += momentum_accelerations[interval] * weight
            acceleration_available_weight += weight
        momentum_acceleration = round(
            acceleration_weighted_sum / acceleration_available_weight if acceleration_available_weight else 0.0,
            4,
        )
        capped_acceleration = max(-self.ACCELERATION_CAP, min(self.ACCELERATION_CAP, momentum_acceleration))
        momentum_score = round(raw_momentum_score + capped_acceleration * self.ACCELERATION_WEIGHT, 4)

        latest_times = [payload["updated_at"] for payload in interval_payloads.values() if payload["updated_at"]]
        latest_price = self._latest_price(interval_payloads)
        available_count = sum(1 for payload in interval_payloads.values() if payload["momentum"] is not None)

        return {
            "rank": 0,
            "symbol": symbol,
            "price": latest_price,
            "momentum_15m": interval_payloads["15m"]["momentum"],
            "momentum_1h": interval_payloads["1h"]["momentum"],
            "momentum_4h": interval_payloads["4h"]["momentum"],
            "momentum_score": momentum_score,
            "momentum_delta_15m": momentum_deltas["15m"],
            "momentum_delta_1h": momentum_deltas["1h"],
            "momentum_delta_4h": momentum_deltas["4h"],
            "momentum_acceleration_15m": momentum_accelerations["15m"],
            "momentum_acceleration_1h": momentum_accelerations["1h"],
            "momentum_acceleration_4h": momentum_accelerations["4h"],
            "momentum_acceleration": momentum_acceleration,
            "momentum_candle_time_15m": momentum_candle_times["15m"],
            "momentum_candle_time_1h": momentum_candle_times["1h"],
            "momentum_candle_time_4h": momentum_candle_times["4h"],
            "updated_timeframes": updated_timeframes,
            "classification": self._classification(momentum_score),
            "rsi_15m": interval_payloads["15m"]["rsi"],
            "rsi_1h": interval_payloads["1h"]["rsi"],
            "rsi_4h": interval_payloads["4h"]["rsi"],
            "change_15m": interval_payloads["15m"]["change"],
            "change_1h": interval_payloads["1h"]["change"],
            "change_4h": interval_payloads["4h"]["change"],
            "ema_trend_15m": interval_payloads["15m"]["ema_trend"],
            "ema_trend_1h": interval_payloads["1h"]["ema_trend"],
            "ema_trend_4h": interval_payloads["4h"]["ema_trend"],
            "updated_at": max(latest_times) if latest_times else None,
            "data_quality": "complete" if available_count == len(self.INTERVALS) else f"partial:{available_count}/{len(self.INTERVALS)}",
            "structure_15m": structure_15m,
        }


    def _safe_delta(self, current_value: float | None, previous_value: float | None) -> float:
        if current_value is None or previous_value is None:
            return 0.0
        return round(float(current_value) - float(previous_value), 4)

    def _safe_acceleration(self, current_delta: float, previous_delta: float | None) -> float:
        if previous_delta is None:
            return 0.0
        return round(float(current_delta) - float(previous_delta), 4)

    def _structure_15m(self, candles: list[dict[str, Any]]) -> dict[str, Any]:
        if len(candles) < 8:
            return {
                "structure_15m_status": "insufficient_data",
                "structure_15m_bias": "neutral",
                "mss_15m_bearish": False,
                "bos_15m_bearish": False,
                "bos_15m_bullish": False,
                "last_swing_low_15m": None,
                "last_swing_high_15m": None,
                "structure_broken_at": None,
                "structure_reason": "not_enough_15m_candles",
            }

        highs = [float(candle["high"]) for candle in candles]
        lows = [float(candle["low"]) for candle in candles]
        closes = [float(candle["close"]) for candle in candles]
        window = self.STRUCTURE_SWING_WINDOW
        swing_lows: list[tuple[int, float]] = []
        swing_highs: list[tuple[int, float]] = []
        for index in range(window, len(candles) - window):
            low_slice = lows[index - window:index + window + 1]
            high_slice = highs[index - window:index + window + 1]
            if lows[index] == min(low_slice):
                swing_lows.append((index, lows[index]))
            if highs[index] == max(high_slice):
                swing_highs.append((index, highs[index]))

        last_close = closes[-1]
        previous_close = closes[-2]
        last_swing_low = swing_lows[-1][1] if swing_lows else min(lows[:-1])
        last_swing_high = swing_highs[-1][1] if swing_highs else max(highs[:-1])
        last_time = candles[-1].get("ingested_at")
        broken_at = last_time if isinstance(last_time, datetime) else datetime.now(timezone.utc)

        bos_bearish = bool(last_swing_low is not None and last_close < last_swing_low)
        mss_bearish = bool(last_swing_low is not None and previous_close >= last_swing_low and last_close < last_swing_low)
        bos_bullish = bool(last_swing_high is not None and last_close > last_swing_high)

        if bos_bearish or mss_bearish:
            return {
                "structure_15m_status": "broken_bearish",
                "structure_15m_bias": "bearish",
                "mss_15m_bearish": mss_bearish,
                "bos_15m_bearish": bos_bearish,
                "bos_15m_bullish": False,
                "last_swing_low_15m": last_swing_low,
                "last_swing_high_15m": last_swing_high,
                "structure_broken_at": broken_at,
                "structure_reason": "15m_close_below_last_swing_low",
            }

        if bos_bullish:
            status = "valid_bullish"
            bias = "bullish"
            reason = "15m_close_above_last_swing_high"
        elif last_close >= last_swing_low:
            status = "valid"
            bias = "neutral_bullish"
            reason = "15m_structure_holding_above_last_swing_low"
        else:
            status = "unknown"
            bias = "neutral"
            reason = "structure_unclear"

        return {
            "structure_15m_status": status,
            "structure_15m_bias": bias,
            "mss_15m_bearish": False,
            "bos_15m_bearish": False,
            "bos_15m_bullish": bos_bullish,
            "last_swing_low_15m": last_swing_low,
            "last_swing_high_15m": last_swing_high,
            "structure_broken_at": None,
            "structure_reason": reason,
        }

    def _interval_momentum(self, candles: list[dict[str, Any]]) -> dict[str, Any]:
        closed_candles = self._closed_candles(candles)
        if len(closed_candles) < 2:
            return self._empty_interval_payload()

        candles_with_close = [candle for candle in closed_candles if candle.get("close") is not None]
        closes = [float(candle["close"]) for candle in candles_with_close]
        if len(closes) < 2 or closes[0] == 0:
            return self._empty_interval_payload()

        first_close = closes[0]
        latest_close = closes[-1]
        latest_candle = candles_with_close[-1]
        price_change = ((latest_close - first_close) / first_close) * 100
        rsi = self._rsi(closes)
        ema = self._ema(closes, min(self.EMA_PERIOD, len(closes)))
        ema_bonus = 0.0
        ema_trend = "unknown"
        if ema is not None:
            if latest_close > ema:
                ema_bonus = 5.0
                ema_trend = "above_ema"
            elif latest_close < ema:
                ema_bonus = -5.0
                ema_trend = "below_ema"
            else:
                ema_trend = "at_ema"

        rsi_component = ((rsi - 50.0) / 2.0) if rsi is not None else 0.0
        momentum = price_change + rsi_component + ema_bonus
        latest_ingested = latest_candle.get("ingested_at")
        candle_time = self._candle_time(latest_candle)

        return {
            "momentum": round(momentum, 4),
            "change": round(price_change, 4),
            "rsi": round(rsi, 4) if rsi is not None else None,
            "ema_trend": ema_trend,
            "updated_at": latest_ingested if isinstance(latest_ingested, datetime) else None,
            "candle_time": candle_time,
            "price": latest_close,
        }

    def _empty_interval_payload(self) -> dict[str, Any]:
        return {
            "momentum": None,
            "change": None,
            "rsi": None,
            "ema_trend": "insufficient_data",
            "updated_at": None,
            "candle_time": None,
            "price": None,
        }

    def _closed_candles(self, candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        closed: list[dict[str, Any]] = []
        for candle in candles:
            close_time = candle.get("close_time")
            if close_time is None:
                continue
            if isinstance(close_time, datetime):
                if close_time <= datetime.now(timezone.utc):
                    closed.append(candle)
                continue
            if float(close_time) <= now_ms:
                closed.append(candle)
        return closed

    def _candle_time(self, candle: dict[str, Any]) -> datetime | None:
        value = candle.get("open_time") or candle.get("close_time")
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)

    def _latest_price(self, interval_payloads: dict[str, dict[str, Any]]) -> float | None:
        for interval in ("15m", "1h", "4h"):
            price = interval_payloads[interval].get("price")
            if price is not None:
                return float(price)
        return None

    def _rsi(self, closes: list[float]) -> float | None:
        if len(closes) <= self.RSI_PERIOD:
            return None
        changes = [closes[index] - closes[index - 1] for index in range(1, len(closes))]
        recent = changes[-self.RSI_PERIOD:]
        gains = [max(change, 0.0) for change in recent]
        losses = [abs(min(change, 0.0)) for change in recent]
        avg_gain = fmean(gains)
        avg_loss = fmean(losses)
        if avg_loss == 0:
            return 100.0 if avg_gain > 0 else 50.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def _ema(self, values: list[float], period: int) -> float | None:
        if not values or period <= 0:
            return None
        alpha = 2.0 / (period + 1.0)
        ema = values[0]
        for value in values[1:]:
            ema = (value * alpha) + (ema * (1.0 - alpha))
        return ema

    def _classification(self, score: float) -> str:
        if score >= 20:
            return "strong_bull"
        if score >= 10:
            return "bull"
        if score >= 0:
            return "neutral_bull"
        if score >= -10:
            return "neutral_bear"
        return "bear"

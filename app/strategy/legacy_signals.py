from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


def closes(candles: list[dict[str, Any]]) -> list[float]:
    return [c["close"] for c in candles]


def highs(candles: list[dict[str, Any]]) -> list[float]:
    return [c["high"] for c in candles]


def lows(candles: list[dict[str, Any]]) -> list[float]:
    return [c["low"] for c in candles]


def rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) < period + 1:
        return None
    diffs = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(diff, 0.0) for diff in diffs]
    losses = [max(-diff, 0.0) for diff in diffs]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def recent_extremes(candles: list[dict[str, Any]], window: int) -> tuple[float, float]:
    subset = candles[-window:]
    return max(c["high"] for c in subset), min(c["low"] for c in subset)


def _local_dt(ts_ms: int | None, offset_hours: int) -> datetime:
    if not ts_ms:
        return datetime.now(timezone.utc) + timedelta(hours=offset_hours)
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc) + timedelta(hours=offset_hours)


def _session_from_hour(hour: int) -> str:
    if 2 <= hour < 4:
        return "asia"
    if 4 <= hour < 6:
        return "london_open"
    if 6 <= hour < 9:
        return "london"
    if 9 <= hour < 12:
        return "new_york"
    return "off_session"


def current_session(offset_hours: int) -> str:
    now = datetime.now(timezone.utc) + timedelta(hours=offset_hours)
    return _session_from_hour(now.hour)


def session_from_timestamp(ts_ms: int | None, offset_hours: int) -> str:
    return _session_from_hour(_local_dt(ts_ms, offset_hours).hour)


def infer_interval_label(candles: list[dict[str, Any]]) -> str:
    if len(candles) < 2:
        return "unknown"
    delta_ms = candles[-1]["open_time"] - candles[-2]["open_time"]
    minutes = int(round(delta_ms / 60000))
    mapping = {1: "1m", 3: "3m", 5: "5m", 15: "15m", 30: "30m", 60: "1h", 120: "2h", 240: "4h", 360: "6h", 480: "8h", 720: "12h", 1440: "1d"}
    return mapping.get(minutes, f"{minutes}m")


def near_level(price: float, level: float, pct: float) -> bool:
    if level == 0:
        return False
    return abs(price - level) / level <= pct


def session_extremes(candles: list[dict[str, Any]], offset_hours: int, session_name: str) -> tuple[float | None, float | None]:
    selected = [c for c in candles if session_from_timestamp(c["open_time"], offset_hours) == session_name]
    if not selected:
        return None, None
    return max(c["high"] for c in selected), min(c["low"] for c in selected)


def equal_highs_lows(candles: list[dict[str, Any]], tolerance_pct: float, lookback: int = 20) -> dict[str, bool]:
    subset = candles[-lookback:]
    hs = highs(subset)
    ls = lows(subset)
    eqh = False
    eql = False
    if len(hs) >= 2:
        top = max(hs)
        near = [h for h in hs if abs(h - top) / top <= tolerance_pct]
        eqh = len(near) >= 2
    if len(ls) >= 2:
        bot = min(ls)
        near = [l for l in ls if abs(l - bot) / bot <= tolerance_pct]
        eql = len(near) >= 2
    return {"equal_highs": eqh, "equal_lows": eql}


def previous_day_extremes(candles: list[dict[str, Any]], offset_hours: int) -> tuple[float | None, float | None]:
    if not candles:
        return None, None
    latest = _local_dt(candles[-1].get("open_time"), offset_hours).date()
    previous = latest - timedelta(days=1)
    selected = [c for c in candles if _local_dt(c.get("open_time"), offset_hours).date() == previous]
    if not selected:
        return None, None
    return max(c["high"] for c in selected), min(c["low"] for c in selected)


def previous_week_extremes(candles: list[dict[str, Any]], offset_hours: int) -> tuple[float | None, float | None]:
    if not candles:
        return None, None
    latest_dt = _local_dt(candles[-1].get("open_time"), offset_hours)
    current_week_start = (latest_dt - timedelta(days=latest_dt.weekday())).date()
    previous_week_start = current_week_start - timedelta(days=7)
    previous_week_end = current_week_start
    selected = []
    for c in candles:
        d = _local_dt(c.get("open_time"), offset_hours).date()
        if previous_week_start <= d < previous_week_end:
            selected.append(c)
    if not selected:
        return None, None
    return max(c["high"] for c in selected), min(c["low"] for c in selected)

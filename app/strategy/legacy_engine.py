from __future__ import annotations

from statistics import mean
from typing import Any

from app.strategy.legacy_signals import (
    closes,
    equal_highs_lows,
    infer_interval_label,
    near_level,
    previous_day_extremes,
    previous_week_extremes,
    recent_extremes,
    rsi,
    session_from_timestamp,
    session_phase_from_timestamp,
    today_session_extremes,
    volumes,
)

ALLOWED_CONFIRM_SESSIONS = {"london_open", "london", "new_york"}


def _find_imbalance_target(candles: list[dict[str, Any]], direction: str, current_price: float, timeframe: str) -> dict[str, Any] | None:
    if len(candles) < 3:
        return None
    candidates: list[dict[str, Any]] = []
    for i in range(2, len(candles)):
        left = candles[i - 2]
        right = candles[i]
        if direction == "up" and float(left["high"]) < float(right["low"]):
            low = float(left["high"])
            high = float(right["low"])
            mid = (low + high) / 2.0
            if mid > current_price:
                candidates.append({"type": f"imbalance_{timeframe}_up", "level": mid, "zone_low": low, "zone_high": high, "reason": f"nearest upside imbalance midpoint on {timeframe}", "timeframe": timeframe})
        elif direction == "down" and float(left["low"]) > float(right["high"]):
            high = float(left["low"])
            low = float(right["high"])
            mid = (low + high) / 2.0
            if mid < current_price:
                candidates.append({"type": f"imbalance_{timeframe}_down", "level": mid, "zone_low": low, "zone_high": high, "reason": f"nearest downside imbalance midpoint on {timeframe}", "timeframe": timeframe})
    if not candidates:
        return None
    return min(candidates, key=lambda x: x["level"]) if direction == "up" else max(candidates, key=lambda x: x["level"])


def _recent_pivot_level(candles: list[dict[str, Any]], *, direction: str, lookback: int, exclude_current: bool = True) -> float | None:
    if not candles:
        return None
    source = candles[:-1] if exclude_current and len(candles) > 1 else candles
    if not source:
        return None
    window = source[-lookback:] if len(source) >= lookback else source
    if not window:
        return None
    if direction == "up":
        return max(float(c["high"]) for c in window)
    return min(float(c["low"]) for c in window)


def _range_bounds(candles: list[dict[str, Any]], lookback: int = 24) -> tuple[float | None, float | None]:
    if not candles:
        return None, None
    source = candles[:-1] if len(candles) > 1 else candles
    if not source:
        return None, None
    window = source[-lookback:] if len(source) >= lookback else source
    if not window:
        return None, None
    return max(float(c["high"]) for c in window), min(float(c["low"]) for c in window)


def _find_old_shelf(candles: list[dict[str, Any]], *, direction: str, current_price: float, timeframe: str, tolerance_pct: float = 0.003, lookback: int = 36, min_touches: int = 2) -> dict[str, Any] | None:
    if not candles:
        return None
    source = candles[:-1] if len(candles) > 1 else candles
    if not source:
        return None
    window = source[-lookback:] if len(source) >= lookback else source
    if len(window) < min_touches:
        return None
    levels = [float(c["high"]) for c in window] if direction == "up" else [float(c["low"]) for c in window]
    best_level = None
    best_touches = 0
    for level in levels:
        if direction == "up" and level <= current_price:
            continue
        if direction == "down" and level >= current_price:
            continue
        touches = sum(1 for other in levels if abs(other - level) / max(abs(level), 1e-9) <= tolerance_pct)
        if touches < min_touches:
            continue
        if best_level is None:
            best_level = level
            best_touches = touches
            continue
        if direction == "up":
            if level < best_level or (abs(level - best_level) / max(abs(best_level), 1e-9) <= tolerance_pct and touches > best_touches):
                best_level = level
                best_touches = touches
        else:
            if level > best_level or (abs(level - best_level) / max(abs(best_level), 1e-9) <= tolerance_pct and touches > best_touches):
                best_level = level
                best_touches = touches
    if best_level is None:
        return None
    return {
        "type": "old_resistance_shelf" if direction == "up" else "old_support_shelf",
        "level": best_level,
        "reason": f"historical {'resistance' if direction == 'up' else 'support'} shelf with repeated reactions on {timeframe}",
        "timeframe": timeframe,
        "touches": best_touches,
    }


def _distance_pct(price: float, level: float | None) -> float | None:
    if price <= 0 or level is None:
        return None
    return abs(level - price) / price


def _infer_htf_watch_bias(*, price: float, rsi_htf: float | None, rsi_macro: float | None, range_high_4h: float | None, range_low_4h: float | None, old_resistance_shelf: dict[str, Any] | None, old_support_shelf: dict[str, Any] | None, near_macro_high: bool, near_macro_low: bool) -> tuple[str, dict[str, int]]:
    bull_score = 0
    bear_score = 0

    if range_high_4h is not None and range_low_4h is not None and range_high_4h > range_low_4h:
        range_span = range_high_4h - range_low_4h
        if range_span > 0:
            range_position = (price - range_low_4h) / range_span
            if range_position <= 0.45:
                bull_score += 3
            elif range_position >= 0.55:
                bear_score += 3

    support_level = old_support_shelf.get("level") if old_support_shelf else None
    resistance_level = old_resistance_shelf.get("level") if old_resistance_shelf else None
    support_distance = _distance_pct(price, support_level)
    resistance_distance = _distance_pct(price, resistance_level)
    if support_distance is not None and resistance_distance is not None:
        if support_distance < resistance_distance:
            bull_score += 1
        elif resistance_distance < support_distance:
            bear_score += 1
    elif support_distance is not None:
        bull_score += 1
    elif resistance_distance is not None:
        bear_score += 1

    if rsi_htf is not None and rsi_macro is not None:
        if rsi_htf >= 50 and rsi_macro >= 45:
            bull_score += 1
        if rsi_htf <= 50 and rsi_macro <= 55:
            bear_score += 1

    if near_macro_low:
        bull_score += 1
    if near_macro_high:
        bear_score += 1

    if bull_score > bear_score:
        return "bull", {"bull": bull_score, "bear": bear_score}
    if bear_score > bull_score:
        return "bear", {"bull": bull_score, "bear": bear_score}
    return "neutral", {"bull": bull_score, "bear": bear_score}


def _resolve_watch_direction(*, spring_watch: bool, utad_watch: bool, htf_watch_bias: str, sweep_up: bool, sweep_down: bool, near_macro_high: bool, near_macro_low: bool, near_htf_high: bool, near_htf_low: bool, rsi_htf: float | None) -> tuple[str, str]:
    local_bear_strong = bool(utad_watch and sweep_up and (near_macro_high or near_htf_high) and rsi_htf is not None and rsi_htf < 50)
    local_bull_strong = bool(spring_watch and sweep_down and (near_macro_low or near_htf_low) and rsi_htf is not None and rsi_htf > 50)

    if spring_watch and not utad_watch:
        if htf_watch_bias == "bear" and not local_bull_strong:
            return "utad_watch", "bear_watch"
        return "spring_watch", "bull_watch"

    if utad_watch and not spring_watch:
        if htf_watch_bias == "bull" and not local_bear_strong:
            return "spring_watch", "bull_watch"
        return "utad_watch", "bear_watch"

    if spring_watch and utad_watch:
        if htf_watch_bias == "bull":
            return "spring_watch", "bull_watch"
        if htf_watch_bias == "bear":
            return "utad_watch", "bear_watch"
        if local_bull_strong and not local_bear_strong:
            return "spring_watch", "bull_watch"
        if local_bear_strong and not local_bull_strong:
            return "utad_watch", "bear_watch"
        return "spring_watch", "bull_watch"

    return "neutral", "neutral"


def _level_is_relevant(*, bias: str, price: float, level: float | None, tolerance_pct: float = 0.003) -> bool:
    if level is None:
        return False
    if price <= 0:
        return True
    if bias.startswith("bear"):
        return level >= price * (1 - tolerance_pct)
    if bias.startswith("bull"):
        return level <= price * (1 + tolerance_pct)
    return True


def _pick_entry_liquidity_context(*, bias: str, price: float, eq: dict[str, bool], today_asia_high: float | None, today_asia_low: float | None, today_london_high: float | None, today_london_low: float | None, high_main: float, low_main: float, high_htf: float | None, low_htf: float | None, high_macro: float | None, low_macro: float | None, imbalance_up_5m: dict[str, Any] | None, imbalance_down_5m: dict[str, Any] | None, previous_day_high: float | None = None, previous_day_low: float | None = None, range_high_4h: float | None = None, range_low_4h: float | None = None, major_swing_high_4h: float | None = None, major_swing_low_4h: float | None = None, old_resistance_shelf: dict[str, Any] | None = None, old_support_shelf: dict[str, Any] | None = None) -> dict[str, Any]:
    if bias in {"bear_watch", "bear_confirm"}:
        candidates = (
            ({"type": "previous_day_high", "level": previous_day_high, "reason": "previous day high used as primary sell entry context", "timeframe": "1d", "scope": "entry"} if previous_day_high is not None else None),
            ({"type": "range_high", "level": range_high_4h, "reason": "4h range high used as secondary sell entry context", "timeframe": "4h", "scope": "entry"} if range_high_4h is not None else None),
            ({**old_resistance_shelf, "scope": "entry"} if old_resistance_shelf else None),
            ({"type": "major_swing_high_4h", "level": major_swing_high_4h, "reason": "major 4h swing high used as structural sell entry context", "timeframe": "4h", "scope": "entry"} if major_swing_high_4h is not None else None),
            ({"type": "recent_high_4h", "level": high_macro, "reason": "4h high used as secondary sell entry context", "timeframe": "4h", "scope": "entry"} if high_macro is not None else None),
            ({"type": "recent_high_1h", "level": high_htf, "reason": "1h high used as tertiary sell entry context", "timeframe": "1h", "scope": "entry"} if high_htf is not None else None),
            ({"type": "equal_highs_5m", "level": high_main, "reason": "visible buy-side liquidity above equal highs", "timeframe": "5m", "scope": "entry"} if eq["equal_highs"] else None),
            ({**imbalance_up_5m, "scope": "entry"} if imbalance_up_5m else None),
            ({"type": "recent_high_5m", "level": high_main, "reason": "recent visible 5m high liquidity", "timeframe": "5m", "scope": "entry"} if high_main is not None else None),
            ({"type": "today_london_high", "level": today_london_high, "reason": "today london high kept as timing context fallback", "timeframe": "session", "scope": "entry"} if today_london_high is not None else None),
            ({"type": "today_asia_high", "level": today_asia_high, "reason": "today asia high kept as timing context fallback", "timeframe": "session", "scope": "entry"} if today_asia_high is not None else None),
        )
        for item in candidates:
            if item is not None and _level_is_relevant(bias=bias, price=price, level=item.get("level")):
                return item
    if bias in {"bull_watch", "bull_confirm"}:
        candidates = (
            ({"type": "previous_day_low", "level": previous_day_low, "reason": "previous day low used as primary buy entry context", "timeframe": "1d", "scope": "entry"} if previous_day_low is not None else None),
            ({"type": "range_low", "level": range_low_4h, "reason": "4h range low used as secondary buy entry context", "timeframe": "4h", "scope": "entry"} if range_low_4h is not None else None),
            ({**old_support_shelf, "scope": "entry"} if old_support_shelf else None),
            ({"type": "major_swing_low_4h", "level": major_swing_low_4h, "reason": "major 4h swing low used as structural buy entry context", "timeframe": "4h", "scope": "entry"} if major_swing_low_4h is not None else None),
            ({"type": "recent_low_4h", "level": low_macro, "reason": "4h low used as secondary buy entry context", "timeframe": "4h", "scope": "entry"} if low_macro is not None else None),
            ({"type": "recent_low_1h", "level": low_htf, "reason": "1h low used as tertiary buy entry context", "timeframe": "1h", "scope": "entry"} if low_htf is not None else None),
            ({"type": "equal_lows_5m", "level": low_main, "reason": "visible sell-side liquidity below equal lows", "timeframe": "5m", "scope": "entry"} if eq["equal_lows"] else None),
            ({**imbalance_down_5m, "scope": "entry"} if imbalance_down_5m else None),
            ({"type": "recent_low_5m", "level": low_main, "reason": "recent visible 5m low liquidity", "timeframe": "5m", "scope": "entry"} if low_main is not None else None),
            ({"type": "today_london_low", "level": today_london_low, "reason": "today london low kept as timing context fallback", "timeframe": "session", "scope": "entry"} if today_london_low is not None else None),
            ({"type": "today_asia_low", "level": today_asia_low, "reason": "today asia low kept as timing context fallback", "timeframe": "session", "scope": "entry"} if today_asia_low is not None else None),
        )
        for item in candidates:
            if item is not None and _level_is_relevant(bias=bias, price=price, level=item.get("level")):
                return item
    return {"type": "none", "level": None, "reason": "no clear entry liquidity context", "timeframe": None, "scope": "entry"}


def _pick_macro_liquidity_context(*, bias: str, price: float, eq_main: dict[str, bool], eq_htf: dict[str, bool], eq_macro: dict[str, bool], previous_day_high: float | None, previous_day_low: float | None, previous_week_high: float | None, previous_week_low: float | None, today_asia_high: float | None, today_asia_low: float | None, today_london_high: float | None, today_london_low: float | None, high_main: float, low_main: float, high_htf: float | None, low_htf: float | None, high_macro: float | None, low_macro: float | None, imbalance_up_4h: dict[str, Any] | None, imbalance_down_4h: dict[str, Any] | None, imbalance_up_1h: dict[str, Any] | None, imbalance_down_1h: dict[str, Any] | None, range_high_4h: float | None = None, range_low_4h: float | None = None, major_swing_high_4h: float | None = None, major_swing_low_4h: float | None = None, old_resistance_shelf: dict[str, Any] | None = None, old_support_shelf: dict[str, Any] | None = None) -> dict[str, Any]:
    if bias in {"bear_watch", "bear_confirm"}:
        candidates = (
            ({"type": "range_high", "level": range_high_4h, "reason": "4h range high used as primary structural buy-side draw", "timeframe": "4h", "scope": "macro"} if range_high_4h is not None else None),
            ({**old_resistance_shelf, "scope": "macro"} if old_resistance_shelf else None),
            ({"type": "major_swing_high_4h", "level": major_swing_high_4h, "reason": "major 4h swing high used as structural buy-side draw", "timeframe": "4h", "scope": "macro"} if major_swing_high_4h is not None else None),
            ({"type": "previous_day_high", "level": previous_day_high, "reason": "previous day high used as primary buy-side liquidity draw", "timeframe": "1d", "scope": "macro"} if previous_day_high is not None else None),
            ({"type": "equal_highs_4h", "level": high_macro, "reason": "4h equal highs used as macro buy-side liquidity draw", "timeframe": "4h", "scope": "macro"} if eq_macro["equal_highs"] and high_macro is not None else None),
            ({**imbalance_up_4h, "scope": "macro"} if imbalance_up_4h else None),
            ({"type": "recent_high_4h", "level": high_macro, "reason": "4h swing high used as macro buy-side liquidity draw", "timeframe": "4h", "scope": "macro"} if high_macro is not None else None),
            ({"type": "equal_highs_1h", "level": high_htf, "reason": "1h equal highs used as secondary buy-side liquidity draw", "timeframe": "1h", "scope": "macro"} if eq_htf["equal_highs"] and high_htf is not None else None),
            ({**imbalance_up_1h, "scope": "macro"} if imbalance_up_1h else None),
            ({"type": "recent_high_1h", "level": high_htf, "reason": "1h swing high used as secondary buy-side liquidity draw", "timeframe": "1h", "scope": "macro"} if high_htf is not None else None),
            ({"type": "today_london_high", "level": today_london_high, "reason": "today london high used as session buy-side liquidity draw", "timeframe": "session", "scope": "macro"} if today_london_high is not None else None),
            ({"type": "today_asia_high", "level": today_asia_high, "reason": "today asia high used as session buy-side liquidity draw", "timeframe": "session", "scope": "macro"} if today_asia_high is not None else None),
            ({"type": "equal_highs_5m", "level": high_main, "reason": "equal highs used as visible local buy-side liquidity draw", "timeframe": "5m", "scope": "macro"} if eq_main["equal_highs"] else None),
            ({"type": "recent_high_5m", "level": high_main, "reason": "5m fallback buy-side liquidity draw", "timeframe": "5m", "scope": "macro"} if high_main is not None else None),
            ({"type": "previous_week_high", "level": previous_week_high, "reason": "previous week high used as extended fallback buy-side liquidity draw", "timeframe": "1w", "scope": "macro"} if previous_week_high is not None else None),
        )
        for item in candidates:
            if item is not None and _level_is_relevant(bias=bias, price=price, level=item.get("level")):
                return item
    if bias in {"bull_watch", "bull_confirm"}:
        candidates = (
            ({"type": "range_low", "level": range_low_4h, "reason": "4h range low used as primary structural sell-side draw", "timeframe": "4h", "scope": "macro"} if range_low_4h is not None else None),
            ({**old_support_shelf, "scope": "macro"} if old_support_shelf else None),
            ({"type": "major_swing_low_4h", "level": major_swing_low_4h, "reason": "major 4h swing low used as structural sell-side draw", "timeframe": "4h", "scope": "macro"} if major_swing_low_4h is not None else None),
            ({"type": "previous_day_low", "level": previous_day_low, "reason": "previous day low used as primary sell-side liquidity draw", "timeframe": "1d", "scope": "macro"} if previous_day_low is not None else None),
            ({"type": "equal_lows_4h", "level": low_macro, "reason": "4h equal lows used as macro sell-side liquidity draw", "timeframe": "4h", "scope": "macro"} if eq_macro["equal_lows"] and low_macro is not None else None),
            ({**imbalance_down_4h, "scope": "macro"} if imbalance_down_4h else None),
            ({"type": "recent_low_4h", "level": low_macro, "reason": "4h swing low used as macro sell-side liquidity draw", "timeframe": "4h", "scope": "macro"} if low_macro is not None else None),
            ({"type": "equal_lows_1h", "level": low_htf, "reason": "1h equal lows used as secondary sell-side liquidity draw", "timeframe": "1h", "scope": "macro"} if eq_htf["equal_lows"] and low_htf is not None else None),
            ({**imbalance_down_1h, "scope": "macro"} if imbalance_down_1h else None),
            ({"type": "recent_low_1h", "level": low_htf, "reason": "1h swing low used as secondary sell-side liquidity draw", "timeframe": "1h", "scope": "macro"} if low_htf is not None else None),
            ({"type": "today_london_low", "level": today_london_low, "reason": "today london low used as session sell-side liquidity draw", "timeframe": "session", "scope": "macro"} if today_london_low is not None else None),
            ({"type": "today_asia_low", "level": today_asia_low, "reason": "today asia low used as session sell-side liquidity draw", "timeframe": "session", "scope": "macro"} if today_asia_low is not None else None),
            ({"type": "equal_lows_5m", "level": low_main, "reason": "equal lows used as visible local sell-side liquidity draw", "timeframe": "5m", "scope": "macro"} if eq_main["equal_lows"] else None),
            ({"type": "recent_low_5m", "level": low_main, "reason": "5m fallback sell-side liquidity draw", "timeframe": "5m", "scope": "macro"} if low_main is not None else None),
            ({"type": "previous_week_low", "level": previous_week_low, "reason": "previous week low used as extended fallback sell-side liquidity draw", "timeframe": "1w", "scope": "macro"} if previous_week_low is not None else None),
        )
        for item in candidates:
            if item is not None and _level_is_relevant(bias=bias, price=price, level=item.get("level")):
                return item
    return {"type": "none", "level": None, "reason": "no clear macro liquidity context", "timeframe": None, "scope": "macro"}


def _pick_execution_target(*, bias: str, price: float, previous_day_high: float | None, previous_day_low: float | None, previous_week_high: float | None, previous_week_low: float | None, high_main: float, low_main: float, high_htf: float | None, low_htf: float | None, high_macro: float | None, low_macro: float | None, candles_htf: list[dict[str, Any]], candles_macro: list[dict[str, Any]], range_high_4h: float | None = None, range_low_4h: float | None = None, major_swing_high_4h: float | None = None, major_swing_low_4h: float | None = None, old_resistance_shelf: dict[str, Any] | None = None, old_support_shelf: dict[str, Any] | None = None) -> dict[str, Any]:
    if bias == "bull_confirm":
        return (
            ({"type": "range_high", "level": range_high_4h, "reason": "4h range high used as primary structural execution target", "timeframe": "4h"} if range_high_4h is not None and range_high_4h > price else None)
            or (old_resistance_shelf if old_resistance_shelf and old_resistance_shelf.get("level") is not None and float(old_resistance_shelf["level"]) > price else None)
            or ({"type": "major_swing_high_4h", "level": major_swing_high_4h, "reason": "major 4h swing high used as structural execution target", "timeframe": "4h"} if major_swing_high_4h is not None and major_swing_high_4h > price else None)
            or _find_imbalance_target(candles_macro, "up", price, "4h")
            or ({"type": "recent_high_4h", "level": high_macro, "reason": "4h high used as primary extended execution target", "timeframe": "4h"} if high_macro is not None and high_macro > price else None)
            or ({"type": "previous_day_high", "level": previous_day_high, "reason": "previous day high used as secondary extended execution target", "timeframe": "1d"} if previous_day_high is not None and previous_day_high > price else None)
            or _find_imbalance_target(candles_htf, "up", price, "1h")
            or ({"type": "recent_high_1h", "level": high_htf, "reason": "1h high used as secondary execution target", "timeframe": "1h"} if high_htf is not None and high_htf > price else None)
            or ({"type": "previous_week_high", "level": previous_week_high, "reason": "previous week high used as extended execution target", "timeframe": "1w"} if previous_week_high is not None and previous_week_high > price else None)
            or ({"type": "recent_high_5m", "level": high_main, "reason": "5m fallback high used as execution target", "timeframe": "5m"} if high_main is not None and high_main > price else None)
            or {"type": "none", "level": None, "reason": "no execution target", "timeframe": None}
        )
    if bias == "bear_confirm":
        return (
            ({"type": "range_low", "level": range_low_4h, "reason": "4h range low used as primary structural execution target", "timeframe": "4h"} if range_low_4h is not None and range_low_4h < price else None)
            or (old_support_shelf if old_support_shelf and old_support_shelf.get("level") is not None and float(old_support_shelf["level"]) < price else None)
            or ({"type": "major_swing_low_4h", "level": major_swing_low_4h, "reason": "major 4h swing low used as structural execution target", "timeframe": "4h"} if major_swing_low_4h is not None and major_swing_low_4h < price else None)
            or _find_imbalance_target(candles_macro, "down", price, "4h")
            or ({"type": "recent_low_4h", "level": low_macro, "reason": "4h low used as primary extended execution target", "timeframe": "4h"} if low_macro is not None and low_macro < price else None)
            or ({"type": "previous_day_low", "level": previous_day_low, "reason": "previous day low used as secondary extended execution target", "timeframe": "1d"} if previous_day_low is not None and previous_day_low < price else None)
            or _find_imbalance_target(candles_htf, "down", price, "1h")
            or ({"type": "recent_low_1h", "level": low_htf, "reason": "1h low used as secondary execution target", "timeframe": "1h"} if low_htf is not None and low_htf < price else None)
            or ({"type": "previous_week_low", "level": previous_week_low, "reason": "previous week low used as extended execution target", "timeframe": "1w"} if previous_week_low is not None and previous_week_low < price else None)
            or ({"type": "recent_low_5m", "level": low_main, "reason": "5m fallback low used as execution target", "timeframe": "5m"} if low_main is not None and low_main < price else None)
            or {"type": "none", "level": None, "reason": "no execution target", "timeframe": None}
        )
    return {"type": "none", "level": None, "reason": "no execution target", "timeframe": None}


def _projected_target(bias: str, macro_context: dict[str, Any], execution_target: dict[str, Any]) -> dict[str, Any]:
    if execution_target.get("level") is not None:
        return {**execution_target, "projected": False}
    if bias in {"bull_watch", "bear_watch"} and macro_context.get("level") is not None:
        return {"type": f"projected_{macro_context.get('type')}", "level": macro_context.get("level"), "reason": "macro context projected as expected destination before confirmation", "timeframe": macro_context.get("timeframe"), "projected": True}
    return {"type": "none", "level": None, "reason": "no projected target", "timeframe": None, "projected": False}


def _resolve_structural_stop(*, bias: str, entry_price: float, high_main: float, low_main: float, high_htf: float | None, low_htf: float | None, major_swing_high_4h: float | None = None, major_swing_low_4h: float | None = None, entry_context: dict[str, Any], today_london_high: float | None, today_london_low: float | None, today_asia_high: float | None, today_asia_low: float | None, sweep_up: bool, sweep_down: bool, last_high: float, last_low: float) -> tuple[float | None, str]:
    entry_level = entry_context.get("level") if isinstance(entry_context, dict) else None
    entry_type = entry_context.get("type") if isinstance(entry_context, dict) else None

    if bias == "bull_confirm":
        if low_htf is not None:
            return low_htf, "recent_low_1h"
        if major_swing_low_4h is not None and major_swing_low_4h < entry_price and ((entry_price - major_swing_low_4h) / max(entry_price, 1e-9)) <= 0.04:
            return major_swing_low_4h, "major_swing_low_4h"
        if sweep_down:
            return last_low, "sweep_low_5m"
        if entry_level is not None and any(token in str(entry_type or "") for token in ["low", "lows"]):
            return entry_level, str(entry_type)
        if today_london_low is not None:
            return today_london_low, "today_london_low"
        if today_asia_low is not None:
            return today_asia_low, "today_asia_low"
        return low_main, "recent_low_5m"

    if bias == "bear_confirm":
        if high_htf is not None:
            return high_htf, "recent_high_1h"
        if major_swing_high_4h is not None and major_swing_high_4h > entry_price and ((major_swing_high_4h - entry_price) / max(entry_price, 1e-9)) <= 0.04:
            return major_swing_high_4h, "major_swing_high_4h"
        if sweep_up:
            return last_high, "sweep_high_5m"
        if entry_level is not None and any(token in str(entry_type or "") for token in ["high", "highs"]):
            return entry_level, str(entry_type)
        if today_london_high is not None:
            return today_london_high, "today_london_high"
        if today_asia_high is not None:
            return today_asia_high, "today_asia_high"
        return high_main, "recent_high_5m"

    return None, "none"


def _score_volume(candles_main: list[dict[str, Any]]) -> tuple[int, dict[str, Any]]:
    recent = volumes(candles_main[-20:])
    if len(recent) < 5:
        return 0, {"last": 0.0, "average": 0.0, "ratio": 0.0}
    avg = mean(recent[:-1]) if len(recent) > 1 else mean(recent)
    last = recent[-1]
    ratio = (last / avg) if avg else 0.0
    if ratio >= 2.0:
        return 2, {"last": last, "average": avg, "ratio": ratio}
    if ratio >= 1.2:
        return 1, {"last": last, "average": avg, "ratio": ratio}
    return 0, {"last": last, "average": avg, "ratio": ratio}


def _score_market_quality(candles_main: list[dict[str, Any]], price: float) -> tuple[int, dict[str, Any]]:
    recent = candles_main[-20:]
    if len(recent) < 5 or not price:
        return 0, {"avg_range_pct": 0.0}
    avg_range_pct = mean([(float(c["high"]) - float(c["low"])) / price for c in recent if price > 0])
    if avg_range_pct >= 0.02:
        return 0, {"avg_range_pct": avg_range_pct}
    if 0.003 <= avg_range_pct <= 0.012:
        return 2, {"avg_range_pct": avg_range_pct}
    if avg_range_pct > 0:
        return 1, {"avg_range_pct": avg_range_pct}
    return 0, {"avg_range_pct": avg_range_pct}


def _score_htf_alignment(bias: str, rsi_htf: float | None, rsi_macro: float | None) -> int:
    if rsi_htf is None or rsi_macro is None:
        return 0
    if bias.startswith("bull"):
        if rsi_htf >= 50 and rsi_macro >= 45:
            return 2
        if rsi_htf >= 45:
            return 1
    if bias.startswith("bear"):
        if rsi_htf <= 50 and rsi_macro <= 55:
            return 2
        if rsi_htf <= 55:
            return 1
    return 0


def _score_target_quality(trade: dict[str, Any]) -> int:
    entry = trade.get("entry")
    stop = trade.get("stop")
    target = trade.get("target")
    if entry is None or stop is None or target is None:
        return 0
    risk = abs(entry - stop)
    reward = abs(target - entry)
    if risk <= 0:
        return 0
    rr = reward / risk
    if rr >= 2.0:
        return 2
    if rr >= 1.0:
        return 1
    return 0


def _volume_confirmation_profile(candles_main: list[dict[str, Any]]) -> dict[str, Any]:
    recent = volumes(candles_main[-20:])
    if len(recent) < 5:
        return {"last": 0.0, "average": 0.0, "ratio": 0.0, "confirm_soft_ok": False, "confirm_strong_bonus": False}
    avg = mean(recent[:-1]) if len(recent) > 1 else mean(recent)
    last = recent[-1]
    ratio = (last / avg) if avg else 0.0
    return {
        "last": last,
        "average": avg,
        "ratio": ratio,
        "confirm_soft_ok": ratio >= 1.05,
        "confirm_strong_bonus": ratio >= 1.5,
    }


def build_signal(symbol: str, candles_fast: list[dict[str, Any]], candles_main: list[dict[str, Any]], candles_htf: list[dict[str, Any]], candles_macro: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, Any]:
    price = candles_fast[-1]["close"]
    rsi_main = rsi(closes(candles_main), cfg["rsi_period"])
    rsi_htf = rsi(closes(candles_htf), cfg["rsi_period"])
    rsi_macro = rsi(closes(candles_macro), cfg["rsi_period"])
    high_main, low_main = recent_extremes(candles_main, cfg["swing_window"] * 3)
    high_htf, low_htf = recent_extremes(candles_htf, min(len(candles_htf), cfg["swing_window"] * 6))
    high_macro, low_macro = recent_extremes(candles_macro, min(len(candles_macro), max(24, cfg["swing_window"] * 8)))
    range_high_4h, range_low_4h = _range_bounds(candles_macro, lookback=min(len(candles_macro), max(16, cfg["swing_window"] * 8)))
    major_swing_high_4h = _recent_pivot_level(candles_macro, direction="up", lookback=min(len(candles_macro), max(36, cfg["swing_window"] * 12)))
    major_swing_low_4h = _recent_pivot_level(candles_macro, direction="down", lookback=min(len(candles_macro), max(36, cfg["swing_window"] * 12)))
    old_resistance_shelf = _find_old_shelf(candles_macro, direction="up", current_price=price, timeframe="4h", tolerance_pct=max(cfg.get("equal_level_tolerance_pct", 0.002), 0.002), lookback=min(len(candles_macro), max(36, cfg["swing_window"] * 12)))
    old_support_shelf = _find_old_shelf(candles_macro, direction="down", current_price=price, timeframe="4h", tolerance_pct=max(cfg.get("equal_level_tolerance_pct", 0.002), 0.002), lookback=min(len(candles_macro), max(36, cfg["swing_window"] * 12)))
    prev_high = max(c["high"] for c in candles_main[-6:-1])
    prev_low = min(c["low"] for c in candles_main[-6:-1])
    prev_mid = (prev_high + prev_low) / 2
    last = candles_main[-1]
    prev_bar = candles_main[-2] if len(candles_main) >= 2 else candles_main[-1]
    signal_close_time = last.get("close_time")
    session = session_from_timestamp(signal_close_time, cfg["session_timezone_offset_hours"])
    session_phase = session_phase_from_timestamp(signal_close_time, cfg["session_timezone_offset_hours"])
    near_extreme_pct = cfg["signals"]["price_near_extreme_pct"]
    eq_main = equal_highs_lows(candles_main, cfg["equal_level_tolerance_pct"], lookback=20)
    eq_htf = equal_highs_lows(candles_htf, cfg["equal_level_tolerance_pct"], lookback=min(20, len(candles_htf)))
    eq_macro = equal_highs_lows(candles_macro, cfg["equal_level_tolerance_pct"], lookback=min(20, len(candles_macro)))
    today_asia_high, today_asia_low = today_session_extremes(candles_main, cfg["session_timezone_offset_hours"], "asia")
    today_london_high, today_london_low = today_session_extremes(candles_main, cfg["session_timezone_offset_hours"], "london")
    previous_day_high, previous_day_low = previous_day_extremes(candles_main, cfg["session_timezone_offset_hours"])
    previous_week_high, previous_week_low = previous_week_extremes(candles_htf, cfg["session_timezone_offset_hours"])
    imbalance_up_5m = _find_imbalance_target(candles_main, "up", price, "5m")
    imbalance_down_5m = _find_imbalance_target(candles_main, "down", price, "5m")
    imbalance_up_1h = _find_imbalance_target(candles_htf, "up", price, "1h")
    imbalance_down_1h = _find_imbalance_target(candles_htf, "down", price, "1h")
    imbalance_up_4h = _find_imbalance_target(candles_macro, "up", price, "4h")
    imbalance_down_4h = _find_imbalance_target(candles_macro, "down", price, "4h")
    session_confirm_filter_enabled = bool(cfg.get("session_confirm_filter_enabled", True))

    near_recent_high = near_level(price, high_main, near_extreme_pct)
    near_recent_low = near_level(price, low_main, near_extreme_pct)
    near_htf_high = near_level(price, high_htf, near_extreme_pct * 2) if high_htf else False
    near_htf_low = near_level(price, low_htf, near_extreme_pct * 2) if low_htf else False
    near_macro_high = near_level(price, high_macro, near_extreme_pct * 4) if high_macro else False
    near_macro_low = near_level(price, low_macro, near_extreme_pct * 4) if low_macro else False

    htf_watch_bias, htf_watch_scores = _infer_htf_watch_bias(
        price=price,
        rsi_htf=rsi_htf,
        rsi_macro=rsi_macro,
        range_high_4h=range_high_4h,
        range_low_4h=range_low_4h,
        old_resistance_shelf=old_resistance_shelf,
        old_support_shelf=old_support_shelf,
        near_macro_high=near_macro_high,
        near_macro_low=near_macro_low,
    )

    state = "neutral"
    trigger = "wait"
    bias = "neutral"
    tp_zone = False
    confirm_source = "none"
    confirm_blocked_by_session = False
    pipeline = {"collect": True, "liquidity": False, "zone": False, "confirm": False, "trade": False}
    trade = {"status": "watch", "side": "none", "entry": None, "stop": None, "target": None}

    score_breakdown = {"liquidity": 0, "structure": 0, "confirmation": 0, "session": 0, "quality": 0, "volume": 0, "htf_alignment": 0, "market_quality": 0, "target_quality": 0}
    has_liquidity = any([eq_main["equal_highs"], eq_main["equal_lows"], eq_htf["equal_highs"], eq_htf["equal_lows"], eq_macro["equal_highs"], eq_macro["equal_lows"], near_recent_high, near_recent_low, near_htf_high, near_htf_low, near_macro_high, near_macro_low, previous_day_high is not None, previous_day_low is not None, previous_week_high is not None, previous_week_low is not None])
    if has_liquidity:
        pipeline["liquidity"] = True
        score_breakdown["liquidity"] += 2 if (near_macro_high or near_macro_low or near_htf_high or near_htf_low or previous_week_high is not None or previous_week_low is not None) else 1
        if eq_main["equal_highs"] or eq_main["equal_lows"] or eq_htf["equal_highs"] or eq_htf["equal_lows"] or eq_macro["equal_highs"] or eq_macro["equal_lows"]:
            score_breakdown["liquidity"] += 1

    utad_watch = False
    spring_watch = False
    if rsi_main is not None and rsi_main >= cfg["signals"]["overbought"] and (near_recent_high or near_htf_high or near_macro_high or eq_main["equal_highs"] or eq_htf["equal_highs"] or eq_macro["equal_highs"]):
        utad_watch = True
        score_breakdown["structure"] += 2
    if rsi_main is not None and rsi_main <= cfg["signals"]["oversold"] and (near_recent_low or near_htf_low or near_macro_low or eq_main["equal_lows"] or eq_htf["equal_lows"] or eq_macro["equal_lows"]):
        spring_watch = True
        score_breakdown["structure"] += 2

    sweep_up = last["high"] > prev_high and last["close"] < prev_high
    sweep_down = last["low"] < prev_low and last["close"] > prev_low
    if sweep_up:
        utad_watch = True
        score_breakdown["structure"] += 3
    if sweep_down:
        spring_watch = True
        score_breakdown["structure"] += 3

    internal_bear_pivot_high = _recent_pivot_level(candles_main, direction="up", lookback=6)
    internal_bull_pivot_low = _recent_pivot_level(candles_main, direction="down", lookback=6)
    external_swing_high = _recent_pivot_level(candles_main, direction="up", lookback=18)
    external_swing_low = _recent_pivot_level(candles_main, direction="down", lookback=18)

    mss_bull = bool(spring_watch and internal_bear_pivot_high is not None and float(last["close"]) > float(internal_bear_pivot_high))
    mss_bear = bool(utad_watch and internal_bull_pivot_low is not None and float(last["close"]) < float(internal_bull_pivot_low))
    bos_bull = bool(mss_bull and external_swing_high is not None and float(last["close"]) > float(external_swing_high))
    bos_bear = bool(mss_bear and external_swing_low is not None and float(last["close"]) < float(external_swing_low))

    if mss_bull or mss_bear:
        score_breakdown["structure"] += 1
    if bos_bull or bos_bear:
        score_breakdown["confirmation"] += 2

    zone_quality = "weak"
    if utad_watch or spring_watch:
        state, bias = _resolve_watch_direction(
            spring_watch=spring_watch,
            utad_watch=utad_watch,
            htf_watch_bias=htf_watch_bias,
            sweep_up=sweep_up,
            sweep_down=sweep_down,
            near_macro_high=near_macro_high,
            near_macro_low=near_macro_low,
            near_htf_high=near_htf_high,
            near_htf_low=near_htf_low,
            rsi_htf=rsi_htf,
        )
        pipeline["zone"] = True
        if bos_bull or bos_bear:
            zone_quality = "strong"
        elif (mss_bull or mss_bear) or ((near_macro_high or near_macro_low or near_htf_high or near_htf_low) and (sweep_up or sweep_down)):
            zone_quality = "strong"
        elif (near_recent_high or near_recent_low or eq_main["equal_highs"] or eq_main["equal_lows"]):
            zone_quality = "medium"

    bear_strong_confirm = bos_bear
    bull_strong_confirm = bos_bull
    bear_soft_confirm = mss_bear and (last["close"] < prev_mid and last["close"] < last["open"] and last["close"] < prev_bar["close"])
    bull_soft_confirm = mss_bull and (last["close"] > prev_mid and last["close"] > last["open"] and last["close"] > prev_bar["close"])

    volume_score, volume_debug = _score_volume(candles_main)
    confirm_volume = _volume_confirmation_profile(candles_main)
    score_breakdown["volume"] = volume_score

    confirm_candidate = None
    if bear_strong_confirm:
        confirm_candidate = ("break_down_confirm", "bear_confirm", "5m_bos")
        score_breakdown["confirmation"] = max(score_breakdown["confirmation"], 4 + (1 if confirm_volume["confirm_strong_bonus"] else 0))
    elif bull_strong_confirm:
        confirm_candidate = ("break_up_confirm", "bull_confirm", "5m_bos")
        score_breakdown["confirmation"] = max(score_breakdown["confirmation"], 4 + (1 if confirm_volume["confirm_strong_bonus"] else 0))
    elif bear_soft_confirm and confirm_volume["confirm_soft_ok"]:
        confirm_candidate = ("break_down_confirm_soft", "bear_confirm", "5m_mss_soft")
        score_breakdown["confirmation"] = max(score_breakdown["confirmation"], 2 + (1 if volume_score >= 1 else 0))
    elif bull_soft_confirm and confirm_volume["confirm_soft_ok"]:
        confirm_candidate = ("break_up_confirm_soft", "bull_confirm", "5m_mss_soft")
        score_breakdown["confirmation"] = max(score_breakdown["confirmation"], 2 + (1 if volume_score >= 1 else 0))

    session_confirm_allowed = (session in ALLOWED_CONFIRM_SESSIONS) or (not session_confirm_filter_enabled)
    if session_confirm_allowed:
        score_breakdown["session"] = 1
    if confirm_candidate is not None:
        if session_confirm_allowed:
            trigger, bias, confirm_source = confirm_candidate
            pipeline["confirm"] = True
        else:
            confirm_blocked_by_session = True
            score_breakdown["confirmation"] = max(score_breakdown["confirmation"] - 1, 0)

    if session == "london_open" and rsi_main is not None and rsi_main >= cfg["signals"]["overbought"]:
        tp_zone = True; score_breakdown["quality"] += 1
    if session == "asia" and rsi_main is not None and rsi_main <= cfg["signals"]["oversold"]:
        tp_zone = True; score_breakdown["quality"] += 1
    if session == "new_york" and (near_recent_high or near_recent_low):
        tp_zone = True; score_breakdown["quality"] += 1

    score_breakdown["market_quality"], market_quality_debug = _score_market_quality(candles_main, price)
    score_breakdown["htf_alignment"] = _score_htf_alignment(bias, rsi_htf, rsi_macro)

    macro_liquidity_context = _pick_macro_liquidity_context(
        bias=bias,
        price=price,
        eq_main=eq_main,
        eq_htf=eq_htf,
        eq_macro=eq_macro,
        previous_day_high=previous_day_high,
        previous_day_low=previous_day_low,
        previous_week_high=previous_week_high,
        previous_week_low=previous_week_low,
        today_asia_high=today_asia_high,
        today_asia_low=today_asia_low,
        today_london_high=today_london_high,
        today_london_low=today_london_low,
        high_main=high_main,
        low_main=low_main,
        high_htf=high_htf,
        low_htf=low_htf,
        high_macro=high_macro,
        low_macro=low_macro,
        imbalance_up_4h=imbalance_up_4h,
        imbalance_down_4h=imbalance_down_4h,
        imbalance_up_1h=imbalance_up_1h,
        imbalance_down_1h=imbalance_down_1h,
        range_high_4h=range_high_4h,
        range_low_4h=range_low_4h,
        major_swing_high_4h=major_swing_high_4h,
        major_swing_low_4h=major_swing_low_4h,
        old_resistance_shelf=old_resistance_shelf,
        old_support_shelf=old_support_shelf,
    )
    entry_liquidity_context = _pick_entry_liquidity_context(
        bias=bias,
        price=price,
        eq=eq_main,
        today_asia_high=today_asia_high,
        today_asia_low=today_asia_low,
        today_london_high=today_london_high,
        today_london_low=today_london_low,
        high_main=high_main,
        low_main=low_main,
        high_htf=high_htf,
        low_htf=low_htf,
        high_macro=high_macro,
        low_macro=low_macro,
        imbalance_up_5m=imbalance_up_5m,
        imbalance_down_5m=imbalance_down_5m,
        previous_day_high=previous_day_high,
        previous_day_low=previous_day_low,
        range_high_4h=range_high_4h,
        range_low_4h=range_low_4h,
        major_swing_high_4h=major_swing_high_4h,
        major_swing_low_4h=major_swing_low_4h,
        old_resistance_shelf=old_resistance_shelf,
        old_support_shelf=old_support_shelf,
    )
    execution_target = _pick_execution_target(
        bias=bias,
        price=price,
        previous_day_high=previous_day_high,
        previous_day_low=previous_day_low,
        previous_week_high=previous_week_high,
        previous_week_low=previous_week_low,
        high_main=high_main,
        low_main=low_main,
        high_htf=high_htf,
        low_htf=low_htf,
        high_macro=high_macro,
        low_macro=low_macro,
        candles_htf=candles_htf,
        candles_macro=candles_macro,
        range_high_4h=range_high_4h,
        range_low_4h=range_low_4h,
        major_swing_high_4h=major_swing_high_4h,
        major_swing_low_4h=major_swing_low_4h,
        old_resistance_shelf=old_resistance_shelf,
        old_support_shelf=old_support_shelf,
    )

    trade_target = execution_target.get("level")
    if trigger in {"break_down_confirm", "break_down_confirm_soft"}:
        structural_stop, stop_source = _resolve_structural_stop(
            bias=bias,
            entry_price=price,
            high_main=high_main,
            low_main=low_main,
            high_htf=high_htf,
            low_htf=low_htf,
            major_swing_high_4h=major_swing_high_4h,
            major_swing_low_4h=major_swing_low_4h,
            entry_context=entry_liquidity_context,
            today_london_high=today_london_high,
            today_london_low=today_london_low,
            today_asia_high=today_asia_high,
            today_asia_low=today_asia_low,
            sweep_up=sweep_up,
            sweep_down=sweep_down,
            last_high=float(last["high"]),
            last_low=float(last["low"]),
        )
        trade = {"status": "simulated", "side": "short", "entry": price, "stop": structural_stop, "target": trade_target or low_macro or low_htf or low_main, "stop_source": stop_source}; pipeline["trade"] = True
    elif trigger in {"break_up_confirm", "break_up_confirm_soft"}:
        structural_stop, stop_source = _resolve_structural_stop(
            bias=bias,
            entry_price=price,
            high_main=high_main,
            low_main=low_main,
            high_htf=high_htf,
            low_htf=low_htf,
            major_swing_high_4h=major_swing_high_4h,
            major_swing_low_4h=major_swing_low_4h,
            entry_context=entry_liquidity_context,
            today_london_high=today_london_high,
            today_london_low=today_london_low,
            today_asia_high=today_asia_high,
            today_asia_low=today_asia_low,
            sweep_up=sweep_up,
            sweep_down=sweep_down,
            last_high=float(last["high"]),
            last_low=float(last["low"]),
        )
        trade = {"status": "simulated", "side": "long", "entry": price, "stop": structural_stop, "target": trade_target or high_macro or high_htf or high_main, "stop_source": stop_source}; pipeline["trade"] = True

    score_breakdown["target_quality"] = _score_target_quality(trade)
    projected_target = _projected_target(bias, macro_liquidity_context, execution_target)
    score = sum(score_breakdown.values())

    return {
        "symbol": symbol,
        "session": session,
        "session_phase": session_phase,
        "signal_time": signal_close_time,
        "signal_interval": infer_interval_label(candles_main),
        "price": price,
        "rsi_main": rsi_main,
        "rsi_main_timeframe": "5m",
        "rsi_htf": rsi_htf,
        "rsi_htf_timeframe": "1h",
        "rsi_macro": rsi_macro,
        "rsi_macro_timeframe": "4h",
        "state": state,
        "trigger": trigger,
        "bias": bias,
        "tp_zone": tp_zone,
        "score": score,
        "score_breakdown": score_breakdown,
        "zone_quality": zone_quality,
        "confirm_source": confirm_source,
        "confirm_blocked_by_session": confirm_blocked_by_session,
        "session_confirm_filter_enabled": session_confirm_filter_enabled,
        "pipeline": pipeline,
        "trade": trade,
        "liquidity_context": macro_liquidity_context,
        "macro_liquidity_context": macro_liquidity_context,
        "entry_liquidity_context": entry_liquidity_context,
        "execution_target": execution_target,
        "projected_target": projected_target,
        "previous_day_high": previous_day_high,
        "previous_day_low": previous_day_low,
        "previous_week_high": previous_week_high,
        "previous_week_low": previous_week_low,
        "equal_highs_1h": eq_htf["equal_highs"],
        "equal_lows_1h": eq_htf["equal_lows"],
        "equal_highs_4h": eq_macro["equal_highs"],
        "equal_lows_4h": eq_macro["equal_lows"],
        "range_high_4h": range_high_4h,
        "range_low_4h": range_low_4h,
        "major_swing_high_4h": major_swing_high_4h,
        "major_swing_low_4h": major_swing_low_4h,
        "old_resistance_shelf": old_resistance_shelf,
        "old_support_shelf": old_support_shelf,
        "htf_watch_bias": htf_watch_bias,
        "htf_watch_scores": htf_watch_scores,
        "mss_bull": mss_bull,
        "mss_bear": mss_bear,
        "bos_bull": bos_bull,
        "bos_bear": bos_bear,
        "internal_bear_pivot_high": internal_bear_pivot_high,
        "internal_bull_pivot_low": internal_bull_pivot_low,
        "external_swing_high": external_swing_high,
        "external_swing_low": external_swing_low,
        "volume_debug": {**volume_debug, **confirm_volume},
        "market_quality_debug": market_quality_debug,
    }

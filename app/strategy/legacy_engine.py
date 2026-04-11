from __future__ import annotations

from typing import Any

from app.strategy.legacy_signals import closes, equal_highs_lows, infer_interval_label, near_level, previous_day_extremes, previous_week_extremes, recent_extremes, rsi, session_extremes, session_from_timestamp

ALLOWED_CONFIRM_SESSIONS = {"london_open", "london", "new_york"}


def _pick_entry_liquidity_context(*, bias: str, eq: dict[str, bool], asia_high: float | None, asia_low: float | None, london_high: float | None, london_low: float | None, high_main: float, low_main: float, high_htf: float | None, low_htf: float | None, high_macro: float | None, low_macro: float | None) -> dict[str, Any]:
    if bias in {"bear_watch", "bear_confirm"}:
        if eq["equal_highs"]:
            return {"type": "equal_highs_5m", "level": high_main, "reason": "visible buy-side liquidity above equal highs", "timeframe": "5m", "scope": "entry"}
        if london_high is not None:
            return {"type": "london_high_5m", "level": london_high, "reason": "london high as buy-side entry liquidity context", "timeframe": "5m", "scope": "entry"}
        if asia_high is not None:
            return {"type": "asia_high_5m", "level": asia_high, "reason": "asia high as buy-side entry liquidity context", "timeframe": "5m", "scope": "entry"}
        if high_main:
            return {"type": "recent_high_5m", "level": high_main, "reason": "recent visible 5m high liquidity", "timeframe": "5m", "scope": "entry"}
        if high_htf:
            return {"type": "recent_high_1h", "level": high_htf, "reason": "1h high used as fallback entry liquidity context", "timeframe": "1h", "scope": "entry"}
        return {"type": "recent_high_4h", "level": high_macro, "reason": "4h high used as fallback entry liquidity context", "timeframe": "4h", "scope": "entry"}
    if bias in {"bull_watch", "bull_confirm"}:
        if eq["equal_lows"]:
            return {"type": "equal_lows_5m", "level": low_main, "reason": "visible sell-side liquidity below equal lows", "timeframe": "5m", "scope": "entry"}
        if london_low is not None:
            return {"type": "london_low_5m", "level": london_low, "reason": "london low as sell-side entry liquidity context", "timeframe": "5m", "scope": "entry"}
        if asia_low is not None:
            return {"type": "asia_low_5m", "level": asia_low, "reason": "asia low as sell-side entry liquidity context", "timeframe": "5m", "scope": "entry"}
        if low_main:
            return {"type": "recent_low_5m", "level": low_main, "reason": "recent visible 5m low liquidity", "timeframe": "5m", "scope": "entry"}
        if low_htf:
            return {"type": "recent_low_1h", "level": low_htf, "reason": "1h low used as fallback entry liquidity context", "timeframe": "1h", "scope": "entry"}
        return {"type": "recent_low_4h", "level": low_macro, "reason": "4h low used as fallback entry liquidity context", "timeframe": "4h", "scope": "entry"}
    return {"type": "none", "level": None, "reason": "no clear entry liquidity context", "timeframe": None, "scope": "entry"}


def _pick_macro_liquidity_context(*, bias: str, eq_main: dict[str, bool], eq_htf: dict[str, bool], eq_macro: dict[str, bool], previous_day_high: float | None, previous_day_low: float | None, previous_week_high: float | None, previous_week_low: float | None, asia_high: float | None, asia_low: float | None, london_high: float | None, london_low: float | None, high_main: float, low_main: float, high_htf: float | None, low_htf: float | None, high_macro: float | None, low_macro: float | None) -> dict[str, Any]:
    if bias in {"bear_watch", "bear_confirm"}:
        if previous_week_high is not None:
            return {"type": "previous_week_high", "level": previous_week_high, "reason": "previous week high used as primary buy-side liquidity draw", "timeframe": "1w", "scope": "macro"}
        if previous_day_high is not None:
            return {"type": "previous_day_high", "level": previous_day_high, "reason": "previous day high used as secondary buy-side liquidity draw", "timeframe": "1d", "scope": "macro"}
        if eq_macro["equal_highs"]:
            return {"type": "equal_highs_4h", "level": high_macro, "reason": "4h equal highs used as higher timeframe buy-side liquidity draw", "timeframe": "4h", "scope": "macro"}
        if eq_htf["equal_highs"]:
            return {"type": "equal_highs_1h", "level": high_htf, "reason": "1h equal highs used as higher timeframe buy-side liquidity draw", "timeframe": "1h", "scope": "macro"}
        if high_macro:
            return {"type": "recent_high_4h", "level": high_macro, "reason": "4h macro high used as primary buy-side liquidity draw", "timeframe": "4h", "scope": "macro"}
        if high_htf:
            return {"type": "recent_high_1h", "level": high_htf, "reason": "1h high used as secondary buy-side liquidity draw", "timeframe": "1h", "scope": "macro"}
        if eq_main["equal_highs"]:
            return {"type": "equal_highs_5m", "level": high_main, "reason": "equal highs used as visible local buy-side liquidity draw", "timeframe": "5m", "scope": "macro"}
        if london_high is not None:
            return {"type": "london_high_5m", "level": london_high, "reason": "london high used as session buy-side liquidity draw", "timeframe": "5m", "scope": "macro"}
        if asia_high is not None:
            return {"type": "asia_high_5m", "level": asia_high, "reason": "asia high used as session buy-side liquidity draw", "timeframe": "5m", "scope": "macro"}
        return {"type": "recent_high_5m", "level": high_main, "reason": "5m high fallback buy-side liquidity draw", "timeframe": "5m", "scope": "macro"}
    if bias in {"bull_watch", "bull_confirm"}:
        if previous_week_low is not None:
            return {"type": "previous_week_low", "level": previous_week_low, "reason": "previous week low used as primary sell-side liquidity draw", "timeframe": "1w", "scope": "macro"}
        if previous_day_low is not None:
            return {"type": "previous_day_low", "level": previous_day_low, "reason": "previous day low used as secondary sell-side liquidity draw", "timeframe": "1d", "scope": "macro"}
        if eq_macro["equal_lows"]:
            return {"type": "equal_lows_4h", "level": low_macro, "reason": "4h equal lows used as higher timeframe sell-side liquidity draw", "timeframe": "4h", "scope": "macro"}
        if eq_htf["equal_lows"]:
            return {"type": "equal_lows_1h", "level": low_htf, "reason": "1h equal lows used as higher timeframe sell-side liquidity draw", "timeframe": "1h", "scope": "macro"}
        if low_macro:
            return {"type": "recent_low_4h", "level": low_macro, "reason": "4h macro low used as primary sell-side liquidity draw", "timeframe": "4h", "scope": "macro"}
        if low_htf:
            return {"type": "recent_low_1h", "level": low_htf, "reason": "1h low used as secondary sell-side liquidity draw", "timeframe": "1h", "scope": "macro"}
        if eq_main["equal_lows"]:
            return {"type": "equal_lows_5m", "level": low_main, "reason": "equal lows used as visible local sell-side liquidity draw", "timeframe": "5m", "scope": "macro"}
        if london_low is not None:
            return {"type": "london_low_5m", "level": london_low, "reason": "london low used as session sell-side liquidity draw", "timeframe": "5m", "scope": "macro"}
        if asia_low is not None:
            return {"type": "asia_low_5m", "level": asia_low, "reason": "asia low used as session sell-side liquidity draw", "timeframe": "5m", "scope": "macro"}
        return {"type": "recent_low_5m", "level": low_main, "reason": "5m low fallback sell-side liquidity draw", "timeframe": "5m", "scope": "macro"}
    return {"type": "none", "level": None, "reason": "no clear macro liquidity context", "timeframe": None, "scope": "macro"}


def _find_imbalance_target(candles: list[dict[str, Any]], direction: str, current_price: float, timeframe: str) -> dict[str, Any] | None:
    if len(candles) < 3:
        return None
    candidates: list[dict[str, Any]] = []
    for i in range(2, len(candles)):
        left = candles[i - 2]
        right = candles[i]
        if direction == "up":
            if float(left["high"]) < float(right["low"]):
                low = float(left["high"])
                high = float(right["low"])
                mid = (low + high) / 2.0
                if mid > current_price:
                    candidates.append({"type": f"imbalance_{timeframe}", "level": mid, "zone_low": low, "zone_high": high, "reason": f"nearest upside imbalance midpoint on {timeframe}", "timeframe": timeframe})
        else:
            if float(left["low"]) > float(right["high"]):
                high = float(left["low"])
                low = float(right["high"])
                mid = (low + high) / 2.0
                if mid < current_price:
                    candidates.append({"type": f"imbalance_{timeframe}", "level": mid, "zone_low": low, "zone_high": high, "reason": f"nearest downside imbalance midpoint on {timeframe}", "timeframe": timeframe})
    if not candidates:
        return None
    return min(candidates, key=lambda x: x["level"]) if direction == "up" else max(candidates, key=lambda x: x["level"])


def _pick_execution_target(*, bias: str, price: float, high_main: float, low_main: float, high_htf: float | None, low_htf: float | None, high_macro: float | None, low_macro: float | None, candles_htf: list[dict[str, Any]], candles_macro: list[dict[str, Any]]) -> dict[str, Any]:
    if bias == "bull_confirm":
        return _find_imbalance_target(candles_macro, "up", price, "4h") or _find_imbalance_target(candles_htf, "up", price, "1h") or ({"type": "recent_high_4h", "level": high_macro, "reason": "4h high used as execution target", "timeframe": "4h"} if high_macro else None) or ({"type": "recent_high_1h", "level": high_htf, "reason": "1h high used as execution target", "timeframe": "1h"} if high_htf else None) or {"type": "recent_high_5m", "level": high_main, "reason": "5m fallback high used as execution target", "timeframe": "5m"}
    if bias == "bear_confirm":
        return _find_imbalance_target(candles_macro, "down", price, "4h") or _find_imbalance_target(candles_htf, "down", price, "1h") or ({"type": "recent_low_4h", "level": low_macro, "reason": "4h low used as execution target", "timeframe": "4h"} if low_macro else None) or ({"type": "recent_low_1h", "level": low_htf, "reason": "1h low used as execution target", "timeframe": "1h"} if low_htf else None) or {"type": "recent_low_5m", "level": low_main, "reason": "5m fallback low used as execution target", "timeframe": "5m"}
    return {"type": "none", "level": None, "reason": "no execution target", "timeframe": None}


def build_signal(symbol: str, candles_fast: list[dict[str, Any]], candles_main: list[dict[str, Any]], candles_htf: list[dict[str, Any]], candles_macro: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, Any]:
    price = candles_fast[-1]["close"]
    rsi_main = rsi(closes(candles_main), cfg["rsi_period"])
    rsi_htf = rsi(closes(candles_htf), cfg["rsi_period"])
    rsi_macro = rsi(closes(candles_macro), cfg["rsi_period"])
    high_main, low_main = recent_extremes(candles_main, cfg["swing_window"] * 3)
    high_htf, low_htf = recent_extremes(candles_htf, min(len(candles_htf), cfg["swing_window"] * 6))
    high_macro, low_macro = recent_extremes(candles_macro, min(len(candles_macro), max(24, cfg["swing_window"] * 8)))
    prev_high = max(c["high"] for c in candles_main[-6:-1])
    prev_low = min(c["low"] for c in candles_main[-6:-1])
    prev_mid = (prev_high + prev_low) / 2
    last = candles_main[-1]
    prev_bar = candles_main[-2] if len(candles_main) >= 2 else candles_main[-1]
    signal_close_time = last.get("close_time")
    session = session_from_timestamp(signal_close_time, cfg["session_timezone_offset_hours"])
    near_extreme_pct = cfg["signals"]["price_near_extreme_pct"]
    eq_main = equal_highs_lows(candles_main, cfg["equal_level_tolerance_pct"], lookback=20)
    eq_htf = equal_highs_lows(candles_htf, cfg["equal_level_tolerance_pct"], lookback=min(20, len(candles_htf)))
    eq_macro = equal_highs_lows(candles_macro, cfg["equal_level_tolerance_pct"], lookback=min(20, len(candles_macro)))
    asia_high, asia_low = session_extremes(candles_main, cfg["session_timezone_offset_hours"], "asia")
    london_high, london_low = session_extremes(candles_main, cfg["session_timezone_offset_hours"], "london")
    previous_day_high, previous_day_low = previous_day_extremes(candles_main, cfg["session_timezone_offset_hours"])
    previous_week_high, previous_week_low = previous_week_extremes(candles_htf, cfg["session_timezone_offset_hours"])
    session_confirm_filter_enabled = bool(cfg.get("session_confirm_filter_enabled", True))

    near_recent_high = near_level(price, high_main, near_extreme_pct)
    near_recent_low = near_level(price, low_main, near_extreme_pct)
    near_htf_high = near_level(price, high_htf, near_extreme_pct * 2) if high_htf else False
    near_htf_low = near_level(price, low_htf, near_extreme_pct * 2) if low_htf else False
    near_macro_high = near_level(price, high_macro, near_extreme_pct * 4) if high_macro else False
    near_macro_low = near_level(price, low_macro, near_extreme_pct * 4) if low_macro else False

    state = "neutral"
    trigger = "wait"
    bias = "neutral"
    tp_zone = False
    confirm_source = "none"
    confirm_blocked_by_session = False
    pipeline = {"collect": True, "liquidity": False, "zone": False, "confirm": False, "trade": False}
    trade = {"status": "watch", "side": "none", "entry": None, "stop": None, "target": None}

    score_breakdown = {"liquidity": 0, "structure": 0, "confirmation": 0, "session": 0, "quality": 0}
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

    if utad_watch:
        state = "utad_watch"; bias = "bear_watch"; pipeline["zone"] = True
    if spring_watch:
        state = "spring_watch"; bias = "bull_watch"; pipeline["zone"] = True

    bear_strong_confirm = utad_watch and last["close"] < prev_low
    bull_strong_confirm = spring_watch and last["close"] > prev_high
    bear_soft_confirm = utad_watch and (last["close"] < prev_mid and last["close"] < last["open"] and last["close"] < prev_bar["close"])
    bull_soft_confirm = spring_watch and (last["close"] > prev_mid and last["close"] > last["open"] and last["close"] > prev_bar["close"])

    confirm_candidate = None
    if bear_strong_confirm:
        confirm_candidate = ("break_down_confirm", "bear_confirm", "5m_break"); score_breakdown["confirmation"] = 4
    elif bull_strong_confirm:
        confirm_candidate = ("break_up_confirm", "bull_confirm", "5m_break"); score_breakdown["confirmation"] = 4
    elif bear_soft_confirm:
        confirm_candidate = ("break_down_confirm_soft", "bear_confirm", "5m_soft"); score_breakdown["confirmation"] = 2
    elif bull_soft_confirm:
        confirm_candidate = ("break_up_confirm_soft", "bull_confirm", "5m_soft"); score_breakdown["confirmation"] = 2

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

    macro_liquidity_context = _pick_macro_liquidity_context(bias=bias, eq_main=eq_main, eq_htf=eq_htf, eq_macro=eq_macro, previous_day_high=previous_day_high, previous_day_low=previous_day_low, previous_week_high=previous_week_high, previous_week_low=previous_week_low, asia_high=asia_high, asia_low=asia_low, london_high=london_high, london_low=london_low, high_main=high_main, low_main=low_main, high_htf=high_htf, low_htf=low_htf, high_macro=high_macro, low_macro=low_macro)
    entry_liquidity_context = _pick_entry_liquidity_context(bias=bias, eq=eq_main, asia_high=asia_high, asia_low=asia_low, london_high=london_high, london_low=london_low, high_main=high_main, low_main=low_main, high_htf=high_htf, low_htf=low_htf, high_macro=high_macro, low_macro=low_macro)
    execution_target = _pick_execution_target(bias=bias, price=price, high_main=high_main, low_main=low_main, high_htf=high_htf, low_htf=low_htf, high_macro=high_macro, low_macro=low_macro, candles_htf=candles_htf, candles_macro=candles_macro)

    trade_target = execution_target.get("level")
    if trigger in {"break_down_confirm", "break_down_confirm_soft"}:
        trade = {"status": "simulated", "side": "short", "entry": price, "stop": high_main, "target": trade_target or low_macro or low_htf or low_main}; pipeline["trade"] = True
    elif trigger in {"break_up_confirm", "break_up_confirm_soft"}:
        trade = {"status": "simulated", "side": "long", "entry": price, "stop": low_main, "target": trade_target or high_macro or high_htf or high_main}; pipeline["trade"] = True

    score = sum(score_breakdown.values())
    return {
        "symbol": symbol,
        "session": session,
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
        "confirm_source": confirm_source,
        "confirm_blocked_by_session": confirm_blocked_by_session,
        "session_confirm_filter_enabled": session_confirm_filter_enabled,
        "pipeline": pipeline,
        "trade": trade,
        "liquidity_context": macro_liquidity_context,
        "macro_liquidity_context": macro_liquidity_context,
        "entry_liquidity_context": entry_liquidity_context,
        "execution_target": execution_target,
        "previous_day_high": previous_day_high,
        "previous_day_low": previous_day_low,
        "previous_week_high": previous_week_high,
        "previous_week_low": previous_week_low,
        "equal_highs_1h": eq_htf["equal_highs"],
        "equal_lows_1h": eq_htf["equal_lows"],
        "equal_highs_4h": eq_macro["equal_highs"],
        "equal_lows_4h": eq_macro["equal_lows"],
    }

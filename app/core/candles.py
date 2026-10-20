"""Closed-bar policy for normalized OHLCV inputs (numeric times are UTC ms)."""
from datetime import datetime, timezone
from math import isfinite
from typing import Any


def epoch_ms(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        if isinstance(value, str) and not value.replace(".", "", 1).isdigit():
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if isinstance(value, datetime):
            value = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
            stamp = value.timestamp() * 1000
        else:
            stamp = float(value)
        return stamp if isfinite(stamp) and stamp >= 0 else None
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def closed_candles(candles: list[dict], *, as_of: datetime | None = None) -> list[dict]:
    """Require a verifiable close; an explicit open flag always vetoes it.

    A true flag alone is insufficient. Missing/invalid close times fail closed.
    Providers that only supply bar-open times must normalize them beforehand
    using their known interval, as the stock/ETF adapter does.
    """
    cutoff = epoch_ms(as_of if as_of is not None else datetime.now(timezone.utc))
    if cutoff is None:
        return []
    result = []
    for candle in candles:
        flag = candle.get("is_closed")
        if flag is not None and str(flag).strip().lower() not in {"true", "1"}:
            continue
        closed = epoch_ms(candle.get("close_time"))
        opened = epoch_ms(candle.get("open_time"))
        if closed is None or closed > cutoff:
            continue
        if "open_time" in candle and (opened is None or closed < opened):
            continue
        result.append(candle)
    return result


def closed_bundle(bundle: dict[str, list[dict]], *, as_of: datetime | None = None) -> dict[str, list[dict]]:
    cutoff = as_of if as_of is not None else datetime.now(timezone.utc)
    return {tf: closed_candles(rows or [], as_of=cutoff) for tf, rows in bundle.items()}

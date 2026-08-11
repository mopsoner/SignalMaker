from raspberry_executor.kraken_client import KrakenClient
from raspberry_executor.config import load_settings


def as_float(value):
    try:
        return float(value)
    except Exception:
        return None


def close_price(row):
    payload = row.get("close_payload") or {}
    if not isinstance(payload, dict):
        return None
    qty = as_float(payload.get("executedQty"))
    quote = as_float(payload.get("cummulativeQuoteQty"))
    if qty and quote:
        return quote / qty
    return None


def pnl_for_position(row, price_cache=None, is_closed=False):
    qty = as_float(row.get("quantity"))
    entry = as_float(row.get("entry_price"))
    symbol = str(row.get("execution_symbol") or row.get("signal_symbol") or "").upper()
    side = str(row.get("side") or "long").lower()
    if qty is None or entry is None or entry <= 0 or not symbol:
        return {"mark_price": None, "pnl": None, "pnl_pct": None}

    mark = close_price(row) if is_closed else None
    if mark is None and not is_closed:
        price_cache = price_cache if price_cache is not None else {}
        if symbol in price_cache:
            mark = price_cache[symbol]
        else:
            settings = load_settings()
            client = KrakenClient(settings.kraken_base_url, settings.kraken_api_key, settings.kraken_secret_key, dry_run=settings.dry_run)
            try:
                mark = client.current_price(symbol)
            except Exception:
                mark = None
            price_cache[symbol] = mark
    if mark is None:
        return {"mark_price": None, "pnl": None, "pnl_pct": None}
    direction = -1 if side == "short" else 1
    pnl = (mark - entry) * qty * direction
    pnl_pct = ((mark - entry) / entry) * 100 * direction
    return {"mark_price": mark, "pnl": pnl, "pnl_pct": pnl_pct}


def fmt(value, digits=4, suffix=""):
    if value is None:
        return ""
    try:
        return f"{float(value):.{digits}f}{suffix}"
    except Exception:
        return str(value)

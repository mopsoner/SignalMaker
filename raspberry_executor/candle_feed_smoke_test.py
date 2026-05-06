import json
import os
import sys
from typing import Any

from raspberry_executor.candle_auto_feed import resolve_feed_symbols
from raspberry_executor.candle_push_once import fetch_klines
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ensure_env
from raspberry_executor.signalmaker_client import SignalMakerClient


def _find_summary(rows: list[dict[str, Any]], symbol: str, interval: str) -> dict[str, Any] | None:
    symbol = symbol.upper()
    for row in rows:
        if str(row.get("symbol", "")).upper() == symbol and row.get("interval") == interval:
            return row
    return None


def _fail(result: dict[str, Any], check: dict[str, Any]) -> int:
    result["status"] = "failed"
    result["checks"].append(check)
    print(json.dumps(result, indent=2))
    return 1


def main() -> int:
    ensure_env()
    settings = load_settings()
    intervals = [item.strip() for item in os.getenv("CANDLE_FEED_INTERVALS", "15m,1h,4h").split(",") if item.strip()]
    limit = int(os.getenv("CANDLE_FEED_LIMIT", "120"))
    smoke_symbol_limit = int(os.getenv("CANDLE_FEED_SMOKE_SYMBOL_LIMIT", "3"))

    result: dict[str, Any] = {
        "status": "pending",
        "signalmaker_base_url": settings.signalmaker_base_url,
        "gateway_id": settings.gateway_id,
        "allowed_symbols": settings.allowed_symbols,
        "intervals": intervals,
        "limit": limit,
        "checks": [],
    }

    client = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)

    try:
        summary_probe = client.candle_summary("BTCUSDT")
        result["checks"].append({
            "name": "signalmaker_get_summary",
            "ok": True,
            "url": client._url("/api/v1/market-data/candles/summary"),
            "row_count": len(summary_probe),
        })
    except Exception as exc:
        return _fail(result, {
            "name": "signalmaker_get_summary",
            "ok": False,
            "url": client._url("/api/v1/market-data/candles/summary"),
            "error": str(exc),
            "hint": "Check SIGNALMAKER_BASE_URL. Use the public Replit HTTPS URL without :8080 and without a typo in the domain.",
        })

    endpoint_check = client.check_candle_ingest_endpoint()
    result["checks"].append({"name": "signalmaker_post_candles_probe", **endpoint_check})
    if not endpoint_check.get("ok"):
        result["status"] = "failed"
        result["hint"] = "The Raspberry URL/port may be correct if GET worked, but the deployed SignalMaker backend is not accepting POST /api/v1/market-data/candles yet. Pull main and restart Replit."
        print(json.dumps(result, indent=2))
        return 1

    try:
        symbols, quote_assets = resolve_feed_symbols(settings)
    except Exception as exc:
        return _fail(result, {
            "name": "resolve_feed_symbols",
            "ok": False,
            "error": str(exc),
            "hint": "Check BINANCE_BASE_URL and ALLOWED_SYMBOLS. Example: ALLOWED_SYMBOLS=USDT or ALLOWED_SYMBOLS=BTCUSDT,ETHUSDT.",
        })

    selected_symbols = symbols[:smoke_symbol_limit]
    result.update({
        "quote_assets": quote_assets,
        "discovered_symbol_count": len(symbols),
        "tested_symbol_count": len(selected_symbols),
        "tested_symbols": selected_symbols,
    })

    if not selected_symbols:
        return _fail(result, {"name": "resolve_feed_symbols", "ok": False, "error": "no_symbols_resolved"})

    result["checks"].append({
        "name": "resolve_feed_symbols",
        "ok": True,
        "symbol_count": len(symbols),
        "tested_symbols": selected_symbols,
        "quote_assets": quote_assets,
    })

    pushed = []
    errors = []

    for symbol in selected_symbols:
        for interval in intervals:
            try:
                before_summary = _find_summary(client.candle_summary(symbol), symbol, interval)
                before_count = int(before_summary.get("candle_count", 0)) if before_summary else 0

                candles = fetch_klines(settings.binance_base_url, symbol, interval, limit)
                if not candles:
                    errors.append({"symbol": symbol, "interval": interval, "error": "no_candles_returned"})
                    continue

                ingest = client.post_candles(symbol, interval, candles, source=f"{settings.gateway_id}-feed-smoke-test")
                after_summary = _find_summary(client.candle_summary(symbol), symbol, interval)
                after_count = int(after_summary.get("candle_count", 0)) if after_summary else 0

                ok = ingest.get("status") == "ok" and after_summary is not None and after_count >= before_count
                item = {
                    "symbol": symbol,
                    "interval": interval,
                    "ok": ok,
                    "fetched": len(candles),
                    "upserted": ingest.get("upserted"),
                    "before_count": before_count,
                    "after_count": after_count,
                    "last_close": after_summary.get("last_close") if after_summary else None,
                }
                pushed.append(item)
                if not ok:
                    errors.append({"symbol": symbol, "interval": interval, "error": "ingest_not_visible", "item": item})
            except Exception as exc:
                errors.append({"symbol": symbol, "interval": interval, "error": str(exc)})

    result["pushed"] = pushed
    result["errors"] = errors
    result["checks"].append({
        "name": "feed_3_timeframes",
        "ok": not errors,
        "pushed_count": len(pushed),
        "error_count": len(errors),
    })

    result["status"] = "ok" if not errors else "failed"
    print(json.dumps(result, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(main())

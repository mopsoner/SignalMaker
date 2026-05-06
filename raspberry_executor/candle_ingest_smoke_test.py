import json
import sys
from typing import Any

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


def main() -> int:
    ensure_env()
    settings = load_settings()
    symbol = (sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT").upper()
    interval = sys.argv[2] if len(sys.argv) > 2 else "15m"
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else 20

    client = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    result: dict[str, Any] = {
        "status": "pending",
        "signalmaker_base_url": settings.signalmaker_base_url,
        "gateway_id": settings.gateway_id,
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
        "checks": [],
    }

    before_rows = client.candle_summary(symbol)
    before = _find_summary(before_rows, symbol, interval)
    before_count = int(before.get("candle_count", 0)) if before else 0
    before_last_close = before.get("last_close") if before else None
    result["before"] = before or {}
    result["checks"].append({"name": "summary_before", "ok": True, "candle_count": before_count, "last_close": before_last_close})

    candles = fetch_klines(settings.binance_base_url, symbol, interval, limit)
    if not candles:
        result["status"] = "failed"
        result["checks"].append({"name": "fetch_klines", "ok": False, "error": "no_candles_returned"})
        print(json.dumps(result, indent=2))
        return 1
    result["checks"].append({
        "name": "fetch_klines",
        "ok": True,
        "count": len(candles),
        "first_open_time": candles[0]["open_time"],
        "last_close_time": candles[-1]["close_time"],
    })

    ingest = client.post_candles(symbol, interval, candles, source=f"{settings.gateway_id}-smoke-test")
    result["ingest"] = ingest
    ingest_ok = ingest.get("status") == "ok" and int(ingest.get("upserted", 0)) == len(candles)
    result["checks"].append({"name": "post_candles", "ok": ingest_ok, "response": ingest})
    if not ingest_ok:
        result["status"] = "failed"
        print(json.dumps(result, indent=2))
        return 1

    after_rows = client.candle_summary(symbol)
    after = _find_summary(after_rows, symbol, interval)
    after_count = int(after.get("candle_count", 0)) if after else 0
    after_last_close = after.get("last_close") if after else None
    result["after"] = after or {}

    count_ok = after_count >= before_count
    summary_ok = after is not None and count_ok
    result["checks"].append({
        "name": "summary_after",
        "ok": summary_ok,
        "before_count": before_count,
        "after_count": after_count,
        "before_last_close": before_last_close,
        "after_last_close": after_last_close,
    })

    if not summary_ok:
        result["status"] = "failed"
        print(json.dumps(result, indent=2))
        return 1

    result["status"] = "ok"
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

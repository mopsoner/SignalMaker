import sys

import requests

from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ensure_env
from raspberry_executor.signalmaker_client import SignalMakerClient


def fetch_klines(base_url: str, symbol: str, interval: str, limit: int, start_time: int | None = None) -> list[dict]:
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    if start_time is not None:
        params["startTime"] = int(start_time)
    response = requests.get(
        f"{base_url.rstrip('/')}/api/v3/klines",
        params=params,
        timeout=20,
    )
    response.raise_for_status()
    rows = response.json()
    candles = []
    for row in rows:
        candles.append({
            "open_time": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
            "close_time": int(row[6]),
            "quote_volume": float(row[7]),
            "number_of_trades": int(row[8]),
            "taker_buy_base_volume": float(row[9]),
            "taker_buy_quote_volume": float(row[10]),
        })
    return candles


def main() -> int:
    ensure_env()
    settings = load_settings()
    symbol = (sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT").upper()
    interval = sys.argv[2] if len(sys.argv) > 2 else "15m"
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else 100

    candles = fetch_klines(settings.binance_base_url, symbol, interval, limit)
    client = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    result = client.post_candles(symbol, interval, candles, source=settings.gateway_id)
    print(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())

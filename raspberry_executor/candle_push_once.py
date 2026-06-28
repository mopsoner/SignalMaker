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


def _kraken_interval_minutes(interval: str) -> int:
    value = str(interval).strip().lower()
    if value.endswith("m"):
        return int(value[:-1])
    if value.endswith("h"):
        return int(value[:-1]) * 60
    if value.endswith("d"):
        return int(value[:-1]) * 1440
    if value.endswith("w"):
        return int(value[:-1]) * 10080
    return int(value)


def fetch_kraken_ohlc(base_url: str, symbol: str, interval: str, limit: int, start_time: int | None = None) -> list[dict]:
    interval_minutes = _kraken_interval_minutes(interval)
    params = {"pair": symbol.upper(), "interval": interval_minutes}
    if start_time is not None:
        params["since"] = int(start_time) // 1000
    response = requests.get(f"{base_url.rstrip()}/0/public/OHLC", params=params, timeout=20)
    response.raise_for_status()
    data = response.json()
    errors = data.get("error") or []
    if errors:
        raise RuntimeError(f"Kraken OHLC failed errors={errors}")
    result = data.get("result") or {}
    rows = []
    for key, value in result.items():
        if key != "last":
            rows = value or []
            break
    rows = rows[-limit:] if limit and limit > 0 else rows
    candles = []
    width_ms = interval_minutes * 60 * 1000
    for row in rows:
        open_time = int(float(row[0]) * 1000)
        close = float(row[4])
        volume = float(row[6])
        candles.append({
            "open_time": open_time,
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": close,
            "volume": volume,
            "close_time": open_time + width_ms - 1,
            "quote_volume": volume * close,
            "number_of_trades": int(row[7]),
            "taker_buy_base_volume": 0.0,
            "taker_buy_quote_volume": 0.0,
        })
    return candles


def fetch_exchange_klines(exchange: str, base_url: str, symbol: str, interval: str, limit: int, start_time: int | None = None) -> list[dict]:
    if str(exchange or "binance").lower() in {"kraken", "kraken_pro"}:
        return fetch_kraken_ohlc(base_url, symbol, interval, limit, start_time=start_time)
    return fetch_klines(base_url, symbol, interval, limit, start_time=start_time)


def main() -> int:
    ensure_env()
    settings = load_settings()
    symbol = (sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT").upper()
    interval = sys.argv[2] if len(sys.argv) > 2 else "15m"
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else 100

    exchange = getattr(settings, "exchange", "binance")
    base_url = settings.kraken_base_url if str(exchange).lower() in {"kraken", "kraken_pro"} else settings.binance_base_url
    candles = fetch_exchange_klines(exchange, base_url, symbol, interval, limit)
    client = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    result = client.post_candles(symbol, interval, candles, source=settings.gateway_id)
    print(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())

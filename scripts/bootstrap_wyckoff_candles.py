#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


KRAKEN_ASSET_PAIRS_URL = "https://api.kraken.com/0/public/AssetPairs"
KRAKEN_OHLC_URL = "https://api.kraken.com/0/public/OHLC"

INTERVAL_TO_KRAKEN_MINUTES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
}


@dataclass(frozen=True)
class KrakenPair:
    pair_key: str
    altname: str
    wsname: str
    base: str
    quote: str
    symbol: str
    leverage_buy: list[int]
    leverage_sell: list[int]


def env_str(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        print(f"[bootstrap] invalid int {name}={raw!r}, fallback={default}")
        return default


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def http_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout: int = 45,
) -> dict[str, Any]:
    data = None
    headers = {"User-Agent": "SignalMaker-Raspberry-Bootstrap/1.0"}

    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} on {url}: {body[:500]}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"URL error on {url}: {exc}") from exc


def normalize_asset(asset: str) -> str:
    asset = (asset or "").upper().strip()

    aliases = {
        "XBT": "BTC",
        "XXBT": "BTC",
        "ZUSD": "USD",
        "XETH": "ETH",
        "XXDG": "DOGE",
    }
    if asset in aliases:
        return aliases[asset]

    # Kraken legacy asset codes often start with X/Z, e.g. XXRP, ZEUR.
    if len(asset) > 3 and asset[0] in {"X", "Z"}:
        stripped = asset[1:]
        if stripped in aliases:
            return aliases[stripped]
        return stripped

    return asset


def quote_matches(pair: dict[str, Any], wanted_quotes: set[str]) -> bool:
    quote = normalize_asset(str(pair.get("quote") or ""))
    wsname = str(pair.get("wsname") or "").upper()

    if quote in wanted_quotes:
        return True

    for wanted in wanted_quotes:
        if wsname.endswith(f"/{wanted}"):
            return True

    return False


def is_non_spot_variant(pair_key: str, pair: dict[str, Any]) -> bool:
    altname = str(pair.get("altname") or pair_key).lower()
    wsname = str(pair.get("wsname") or "").lower()
    text = f"{pair_key} {altname} {wsname}"

    bad_markers = [".d", ".m", ".f", "perp", "future", "futures"]
    return any(marker in text for marker in bad_markers)


def symbol_from_pair(pair_key: str, pair: dict[str, Any]) -> str:
    wsname = str(pair.get("wsname") or "").upper()
    if "/" in wsname:
        base, quote = wsname.split("/", 1)
        return f"{normalize_asset(base)}{normalize_asset(quote)}"

    base = normalize_asset(str(pair.get("base") or ""))
    quote = normalize_asset(str(pair.get("quote") or ""))

    if base and quote:
        return f"{base}{quote}"

    altname = str(pair.get("altname") or pair_key).upper()
    return altname.replace("/", "")


def load_kraken_pairs(wanted_quotes: set[str], margin_only: bool, max_symbols: int) -> list[KrakenPair]:
    data = http_json(KRAKEN_ASSET_PAIRS_URL)
    errors = data.get("error") or []
    if errors:
        raise RuntimeError(f"Kraken AssetPairs error: {errors}")

    result = data.get("result") or {}
    selected: dict[str, KrakenPair] = {}

    kraken_pairs_count = 0
    quote_pairs_count = 0
    margin_pairs_count = 0

    for pair_key, pair in result.items():
        kraken_pairs_count += 1

        status = str(pair.get("status") or "").lower()
        if status and status != "online":
            continue

        if is_non_spot_variant(pair_key, pair):
            continue

        if not quote_matches(pair, wanted_quotes):
            continue

        quote_pairs_count += 1

        leverage_buy = pair.get("leverage_buy") or []
        leverage_sell = pair.get("leverage_sell") or []
        has_margin = bool(leverage_buy or leverage_sell)

        if has_margin:
            margin_pairs_count += 1

        if margin_only and not has_margin:
            continue

        symbol = symbol_from_pair(pair_key, pair)
        if not symbol:
            continue

        kp = KrakenPair(
            pair_key=str(pair_key),
            altname=str(pair.get("altname") or pair_key),
            wsname=str(pair.get("wsname") or ""),
            base=normalize_asset(str(pair.get("base") or "")),
            quote=normalize_asset(str(pair.get("quote") or "")),
            symbol=symbol,
            leverage_buy=[int(x) for x in leverage_buy],
            leverage_sell=[int(x) for x in leverage_sell],
        )

        # Dédoublonnage : garder le premier symbole normalisé.
        selected.setdefault(symbol, kp)

    pairs = list(selected.values())
    pairs.sort(key=lambda p: p.symbol)

    if max_symbols > 0:
        pairs = pairs[:max_symbols]

    print(f"[bootstrap] kraken_pairs_count={kraken_pairs_count}")
    print(f"[bootstrap] quote_pairs_count={quote_pairs_count}")
    print(f"[bootstrap] margin_pairs_count={margin_pairs_count}")
    print(f"[bootstrap] selected_pairs_count={len(pairs)}")

    return pairs


def min_candles_for_interval(interval: str) -> int:
    if interval == "15m":
        return env_int("BOOTSTRAP_MIN_15M", 180)
    if interval == "1h":
        return env_int("BOOTSTRAP_MIN_1H", 180)
    if interval == "4h":
        return env_int("BOOTSTRAP_MIN_4H", 120)
    return env_int("BOOTSTRAP_MIN_DEFAULT", 180)


def fetch_kraken_ohlc(pair: KrakenPair, interval: str, min_candles: int) -> list[dict[str, Any]]:
    interval_minutes = INTERVAL_TO_KRAKEN_MINUTES.get(interval)
    if interval_minutes is None:
        raise ValueError(f"Unsupported interval: {interval}")

    since = int(time.time() - (min_candles + 5) * interval_minutes * 60)

    query = urllib.parse.urlencode(
        {
            "pair": pair.altname or pair.pair_key,
            "interval": interval_minutes,
            "since": since,
        }
    )
    url = f"{KRAKEN_OHLC_URL}?{query}"

    data = http_json(url)
    errors = data.get("error") or []
    if errors:
        raise RuntimeError(f"Kraken OHLC error for {pair.symbol} {interval}: {errors}")

    result = data.get("result") or {}

    rows = None
    for key, value in result.items():
        if key == "last":
            continue
        rows = value
        break

    if not rows:
        return []

    candles: list[dict[str, Any]] = []
    close_delta_seconds = interval_minutes * 60 - 1

    for row in rows[-min_candles:]:
        # Kraken OHLC row:
        # [time, open, high, low, close, vwap, volume, count]
        open_time = int(float(row[0]))
        volume = float(row[6])
        count = int(float(row[7])) if len(row) > 7 else 0

        candles.append(
            {
                "open_time": open_time,
                "close_time": open_time + close_delta_seconds,
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": volume,
                "quote_volume": 0.0,
                "number_of_trades": count,
                "taker_buy_base_volume": 0.0,
                "taker_buy_quote_volume": 0.0,
            }
        )

    return candles


def post_candles(base_url: str, symbol: str, interval: str, candles: list[dict[str, Any]], chunk_size: int) -> int:
    if not candles:
        return 0

    endpoint = base_url.rstrip("/") + "/api/v1/market-data/candles"
    total_upserted = 0

    for start in range(0, len(candles), chunk_size):
        chunk = candles[start : start + chunk_size]
        payload = {
            "symbol": symbol,
            "interval": interval,
            "source": "kraken",
            "candles": chunk,
        }
        response = http_json(endpoint, method="POST", payload=payload)
        total_upserted += int(response.get("upserted") or 0)

    return total_upserted


def sleep_for_rate_limit(last_call_at: float | None, rpm: int) -> float:
    if rpm <= 0:
        return time.monotonic()

    min_delay = 60.0 / float(rpm)
    now = time.monotonic()

    if last_call_at is not None:
        elapsed = now - last_call_at
        if elapsed < min_delay:
            time.sleep(min_delay - elapsed)

    return time.monotonic()


def main() -> int:
    base_url = env_str("SIGNALMAKER_BASE_URL", "https://mysginalmaker.replit.app")
    quote_raw = env_str("BOOTSTRAP_QUOTES", "USD")
    wanted_quotes = {item.strip().upper() for item in quote_raw.split(",") if item.strip()}

    if not wanted_quotes:
        wanted_quotes = {"USD"}

    margin_only = env_bool("BOOTSTRAP_MARGIN_ONLY", True)
    max_symbols = env_int("BOOTSTRAP_MAX_SYMBOLS", 300)
    intervals = [item.strip() for item in env_str("BOOTSTRAP_INTERVALS", "15m,1h,4h").split(",") if item.strip()]
    rpm = env_int("BOOTSTRAP_KRAKEN_RPM", 60)
    post_chunk_size = env_int("BOOTSTRAP_POST_CHUNK_SIZE", 60)

    if post_chunk_size <= 0:
        post_chunk_size = 60

    print("=== bootstrap_wyckoff_candles ===")
    print(f"[bootstrap] base_url={base_url}")
    print(f"[bootstrap] quote_assets={','.join(sorted(wanted_quotes))}")
    print(f"[bootstrap] bootstrap_margin_only={str(margin_only).lower()}")
    print(f"[bootstrap] max_symbols={max_symbols}")
    print(f"[bootstrap] intervals={','.join(intervals)}")
    print(f"[bootstrap] kraken_rpm={rpm}")
    print(f"[bootstrap] post_chunk_size={post_chunk_size}")
    print("=================================")

    pairs = load_kraken_pairs(wanted_quotes, margin_only, max_symbols)

    if not pairs:
        print("[bootstrap] no pairs selected")
        return 0

    print("[bootstrap] selected symbols:")
    for pair in pairs:
        print(
            f"  - {pair.symbol:<12} pair={pair.altname:<14} ws={pair.wsname:<14} "
            f"lev_buy={pair.leverage_buy} lev_sell={pair.leverage_sell}"
        )

    total_fetched = 0
    total_posted = 0
    failures = 0
    last_kraken_call_at: float | None = None

    for pair in pairs:
        for interval in intervals:
            try:
                min_candles = min_candles_for_interval(interval)

                last_kraken_call_at = sleep_for_rate_limit(last_kraken_call_at, rpm)
                candles = fetch_kraken_ohlc(pair, interval, min_candles)

                fetched = len(candles)
                posted = post_candles(base_url, pair.symbol, interval, candles, post_chunk_size)

                total_fetched += fetched
                total_posted += posted

                print(
                    f"[bootstrap] symbol={pair.symbol} interval={interval} "
                    f"fetched={fetched} posted={posted}"
                )

            except Exception as exc:
                failures += 1
                print(f"[bootstrap][ERROR] symbol={pair.symbol} interval={interval}: {exc}", file=sys.stderr)

    print("=== bootstrap summary ===")
    print(f"[bootstrap] pairs={len(pairs)}")
    print(f"[bootstrap] intervals={len(intervals)}")
    print(f"[bootstrap] candles_fetched={total_fetched}")
    print(f"[bootstrap] candles_posted={total_posted}")
    print(f"[bootstrap] failures={failures}")
    print("=========================")

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

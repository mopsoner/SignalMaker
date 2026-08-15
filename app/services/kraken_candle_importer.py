from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import requests
from sqlalchemy.orm import Session

from app.services.market_data_service import MarketDataService


KRAKEN_BASE_URL = "https://api.kraken.com"
KRAKEN_OHLC_MAX_CANDLES = 720
MAX_CATCHUP_PAGES = 100
logger = logging.getLogger(__name__)

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


def normalize_kraken_asset(asset: str | None) -> str:
    value = str(asset or "").upper().strip()
    aliases = {
        "XBT": "BTC",
        "XXBT": "BTC",
        "ZUSD": "USD",
        "ZUSDC": "USDC",
        "XETH": "ETH",
        "XXDG": "DOGE",
    }
    if value in aliases:
        return aliases[value]
    if len(value) > 3 and value[0] in {"X", "Z"}:
        stripped = value[1:]
        return aliases.get(stripped, stripped)
    return value


def kraken_symbol_from_pair(pair_key: str, pair: dict[str, Any]) -> str:
    wsname = str(pair.get("wsname") or "").upper()
    if "/" in wsname:
        base, quote = wsname.split("/", 1)
        return f"{normalize_kraken_asset(base)}{normalize_kraken_asset(quote)}"

    base = normalize_kraken_asset(pair.get("base"))
    quote = normalize_kraken_asset(pair.get("quote"))
    if base and quote:
        return f"{base}{quote}"

    altname = str(pair.get("altname") or pair_key).upper()
    if altname.startswith("XBT"):
        altname = "BTC" + altname.removeprefix("XBT")
    return altname.replace("/", "")


def is_non_spot_variant(pair_key: str, pair: dict[str, Any]) -> bool:
    altname = str(pair.get("altname") or pair_key).lower()
    wsname = str(pair.get("wsname") or "").lower()
    text = f"{pair_key} {altname} {wsname}"
    return any(marker in text for marker in [".d", ".m", ".f", "perp", "future", "futures"])


def fetch_kraken_asset_pairs(base_url: str = KRAKEN_BASE_URL) -> dict[str, Any]:
    response = requests.get(f"{base_url.rstrip('/')}/0/public/AssetPairs", params={"assetVersion": 1}, timeout=20)
    response.raise_for_status()
    data = response.json()
    errors = data.get("error") or []
    if errors:
        raise RuntimeError(f"Kraken AssetPairs failed errors={errors}")
    return data.get("result") or {}


def discover_kraken_pairs(
    *,
    quote_assets: list[str],
    margin_only: bool = True,
    require_margin_sell: bool = False,
    max_symbols: int = 0,
    base_url: str = KRAKEN_BASE_URL,
) -> list[KrakenPair]:
    quotes = {quote.upper().strip() for quote in quote_assets if quote.strip()}
    selected: dict[str, KrakenPair] = {}

    for pair_key, pair in fetch_kraken_asset_pairs(base_url).items():
        status = str(pair.get("status") or "online").lower()
        if status not in {"", "online"}:
            continue
        if is_non_spot_variant(pair_key, pair):
            continue

        quote = normalize_kraken_asset(pair.get("quote"))
        wsname = str(pair.get("wsname") or "").upper()
        if quote not in quotes and not any(wsname.endswith(f"/{wanted}") for wanted in quotes):
            continue

        leverage_buy = [int(x) for x in (pair.get("leverage_buy") or [])]
        leverage_sell = [int(x) for x in (pair.get("leverage_sell") or [])]
        # Kraken can expose pairs that support leveraged buys but not leveraged
        # sells.  The executor feed treated those as margin pairs by default and
        # only excluded them when CANDLE_FEED_REQUIRE_MARGIN_SELL was enabled.
        if margin_only and not leverage_buy:
            continue
        if margin_only and require_margin_sell and not leverage_sell:
            continue

        symbol = kraken_symbol_from_pair(pair_key, pair)
        if not symbol:
            continue

        selected.setdefault(
            symbol,
            KrakenPair(
                pair_key=str(pair_key),
                altname=str(pair.get("altname") or pair_key),
                wsname=str(pair.get("wsname") or ""),
                base=normalize_kraken_asset(pair.get("base")),
                quote=quote,
                symbol=symbol,
                leverage_buy=leverage_buy,
                leverage_sell=leverage_sell,
            ),
        )

    pairs = sorted(selected.values(), key=lambda row: row.symbol)
    return pairs[:max_symbols] if max_symbols and max_symbols > 0 else pairs


def candle_from_kraken_row(row: list[Any], interval_minutes: int) -> dict[str, Any]:
    interval_ms = interval_minutes * 60 * 1000
    open_time_seconds = int(float(row[0]))
    open_time_ms = open_time_seconds * 1000
    close = float(row[4])
    vwap = float(row[5]) if len(row) > 5 else close
    volume = float(row[6]) if len(row) > 6 else 0.0
    count = int(float(row[7])) if len(row) > 7 else 0

    return {
        "open_time": open_time_ms,
        "close_time": open_time_ms + interval_ms - 1,
        "open": float(row[1]),
        "high": float(row[2]),
        "low": float(row[3]),
        "close": close,
        "volume": volume,
        "quote_volume": vwap * volume,
        "number_of_trades": count,
        "taker_buy_base_volume": 0.0,
        "taker_buy_quote_volume": 0.0,
        "provider": "KRAKEN",
        "provider_symbol": None,
        "exchange": "kraken",
    }


def fetch_kraken_ohlc(
    *,
    pair: KrakenPair,
    interval: str,
    limit: int,
    base_url: str = KRAKEN_BASE_URL,
    since_ms: int | None = None,
) -> list[dict[str, Any]]:
    interval_minutes = INTERVAL_TO_KRAKEN_MINUTES.get(interval)
    if interval_minutes is None:
        raise ValueError(f"Unsupported interval: {interval}")

    params: dict[str, Any] = {"pair": pair.altname or pair.pair_key, "interval": interval_minutes}
    if since_ms is not None:
        params["since"] = int(since_ms) // 1000

    response = requests.get(f"{base_url.rstrip('/')}/0/public/OHLC", params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    errors = data.get("error") or []
    if errors:
        raise RuntimeError(f"Kraken OHLC failed symbol={pair.symbol} interval={interval} errors={errors}")

    result = data.get("result") or {}
    rows: list[list[Any]] = []
    for key, value in result.items():
        if key != "last":
            rows = value or []
            break

    if limit and limit > 0:
        # A historical request must consume the oldest rows first or advancing
        # the cursor would silently skip the middle of a long outage.  With no
        # cursor, retain the original "most recent N" bootstrap behaviour.
        rows = rows[:limit] if since_ms is not None else rows[-limit:]

    candles = [candle_from_kraken_row(row, interval_minutes) for row in rows]
    for candle in candles:
        candle["provider_symbol"] = pair.altname or pair.pair_key
    if since_ms is not None:
        candles = [candle for candle in candles if int(candle["open_time"]) >= int(since_ms)]
    return candles


def import_kraken_candles(
    *,
    db: Session,
    quote_assets: list[str] | None = None,
    intervals: list[str] | None = None,
    limit: int = 120,
    max_symbols: int = 0,
    margin_only: bool = True,
    require_margin_sell: bool = False,
    base_url: str = KRAKEN_BASE_URL,
    requests_per_minute: int = 60,
    max_catchup_pages: int = MAX_CATCHUP_PAGES,
) -> dict[str, Any]:
    quote_assets = quote_assets or ["USD"]
    intervals = intervals or ["4h"]

    pairs = discover_kraken_pairs(
        quote_assets=quote_assets,
        margin_only=margin_only,
        require_margin_sell=require_margin_sell,
        max_symbols=max_symbols,
        base_url=base_url,
    )

    service = MarketDataService(db)
    min_delay = 60.0 / max(1, int(requests_per_minute))
    last_request_at: float | None = None

    pushed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for pair in pairs:
        for interval in intervals:
            pages = 0
            caught_up = 0
            try:
                latest = service.list_candles(symbol=pair.symbol, interval=interval, limit=1, latest=True)
                interval_ms = INTERVAL_TO_KRAKEN_MINUTES[interval] * 60_000
                cursor_ms = int(latest[0].close_time) + 1 if latest else None
                # Freeze the target for this cycle.  A candle is eligible only
                # when its complete interval ended before this instant.
                target_ms = int(time.time() * 1000)

                while pages < max(1, int(max_catchup_pages)):
                    now = time.monotonic()
                    if last_request_at is not None:
                        elapsed = now - last_request_at
                        if elapsed < min_delay:
                            time.sleep(min_delay - elapsed)
                    last_request_at = time.monotonic()

                    candles = fetch_kraken_ohlc(
                        pair=pair,
                        interval=interval,
                        limit=min(max(1, int(limit)), KRAKEN_OHLC_MAX_CANDLES),
                        base_url=base_url,
                        since_ms=cursor_ms,
                    )
                    pages += 1
                    closed = [row for row in candles if int(row["close_time"]) < target_ms]
                    if cursor_ms is not None:
                        closed = [row for row in closed if int(row["open_time"]) >= cursor_ms]
                    if not closed:
                        break

                    # Commit every page.  If a later request fails, the next
                    # feed cycle resumes from this persisted boundary.
                    service.upsert_candles(pair.symbol, interval, closed)
                    caught_up += len(closed)
                    next_cursor = int(closed[-1]["open_time"]) + interval_ms
                    if cursor_ms is not None and next_cursor <= cursor_ms:
                        break
                    cursor_ms = next_cursor
                    if cursor_ms >= target_ms or len(candles) < min(max(1, int(limit)), KRAKEN_OHLC_MAX_CANDLES):
                        break

                logger.info(
                    "Kraken candle catch-up symbol=%s interval=%s pages=%s candles=%s",
                    pair.symbol, interval, pages, caught_up,
                )
                if pages >= max(1, int(max_catchup_pages)) and cursor_ms is not None and cursor_ms < target_ms:
                    logger.warning(
                        "Kraken candle catch-up safety limit reached symbol=%s interval=%s pages=%s candles=%s cursor_ms=%s",
                        pair.symbol, interval, pages, caught_up, cursor_ms,
                    )

                if not caught_up:
                    skipped.append({"symbol": pair.symbol, "interval": interval, "reason": "already_up_to_date"})
                    continue
                pushed.append({"symbol": pair.symbol, "interval": interval, "candles": caught_up, "upserted": caught_up, "pages": pages})
            except Exception as exc:
                db.rollback()
                logger.warning(
                    "Kraken candle catch-up interrupted symbol=%s interval=%s pages=%s candles=%s error=%s",
                    pair.symbol, interval, pages, caught_up, exc,
                )
                errors.append({"symbol": pair.symbol, "interval": interval, "pages": pages, "candles": caught_up, "error": str(exc)})

    return {
        "status": "ok" if not errors else "partial",
        "source": "kraken_internal",
        "quote_assets": quote_assets,
        "intervals": intervals,
        "margin_only": margin_only,
        "require_margin_sell": require_margin_sell,
        "symbol_count": len(pairs),
        "pushed_count": len(pushed),
        "skipped_count": len(skipped),
        "error_count": len(errors),
        "pushed": pushed,
        "skipped": skipped,
        "errors": errors,
    }

import logging
import math
import time
from datetime import datetime, timezone
from typing import Any

import requests
from sqlalchemy import select

from app.db.session import SessionLocal
from app.models.market_candle import MarketCandle
from app.services.runtime_settings import load_runtime_settings

logger = logging.getLogger(__name__)

KRAKEN_REQUEST_LIMIT = 60
REQUEST_SAFETY_THRESHOLD = 55
INTERVAL_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
}
PIPELINE_INTERVALS = ("15m", "1h", "4h")
DUE_BASED_INTERVALS = {"1h", "4h"}


class RateLimiter:
    """Small neutral request limiter for Kraken public REST calls.

    Kraken public endpoints do not expose Binance-style minute weight headers.
    We therefore count local requests in a rolling minute and still honor HTTP
    429 ``Retry-After`` when Kraken returns one.
    """

    def __init__(self) -> None:
        self._request_count: int = 0
        self._window_start: float = time.monotonic()

    def record_request(self) -> None:
        now = time.monotonic()
        if now - self._window_start >= 60:
            self._request_count = 0
            self._window_start = now
        self._request_count += 1

    def wait_if_needed(self) -> None:
        now = time.monotonic()
        if now - self._window_start >= 60:
            self._request_count = 0
            self._window_start = now
            return
        if self._request_count >= REQUEST_SAFETY_THRESHOLD:
            elapsed = now - self._window_start
            wait = max(0.0, 61.0 - elapsed)
            if wait > 0:
                logger.warning(
                    "Kraken request throttle %d/%d — pause %.1fs avant reset",
                    self._request_count, KRAKEN_REQUEST_LIMIT, wait,
                )
                time.sleep(wait)
                self._request_count = 0
                self._window_start = time.monotonic()


class CollectorService:
    def __init__(self) -> None:
        runtime = load_runtime_settings()
        self.runtime = runtime
        self.base_url = runtime['kraken']['kraken_base_url'].rstrip('/')
        self.collector_enabled = bool(runtime.get('market_data', runtime['kraken']).get('kraken_collector_enabled', False))
        self.session = requests.Session()
        self._rate = RateLimiter()

    def _stored_symbols(self, limit: int | None = None) -> list[str]:
        db = SessionLocal()
        try:
            stmt = select(MarketCandle.symbol).distinct().order_by(MarketCandle.symbol)
            if limit:
                stmt = stmt.limit(limit)
            return [str(symbol).upper() for symbol in db.scalars(stmt).all()]
        finally:
            db.close()

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        if not self.collector_enabled:
            raise RuntimeError('Kraken collector is disabled; use POST /api/v1/market-data/candles to ingest candles')
        self._rate.wait_if_needed()
        if path.startswith('/api/' + 'v3/'):
            raise RuntimeError(f"Binance endpoint is not supported by Kraken collector: {path}")
        url = f"{self.base_url}{path}"
        for attempt in range(3):
            try:
                res = self.session.get(url, params=params or {}, timeout=15)
            except requests.RequestException:
                if attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                raise

            self._rate.record_request()

            if res.status_code == 418:
                retry_after = int(res.headers.get('Retry-After', 120))
                logger.error(
                    "Kraken IP ban (418) — attente %ds avant reprise", retry_after
                )
                raise RuntimeError(
                    f"Kraken a banni l'IP temporairement (418). "
                    f"Attendez {retry_after}s avant de relancer le pipeline."
                )

            if res.status_code == 429:
                retry_after = int(res.headers.get('Retry-After', 60))
                logger.warning(
                    "Kraken rate-limit (429) — attente %ds (tentative %d/3)",
                    retry_after, attempt + 1,
                )
                time.sleep(retry_after)
                continue

            res.raise_for_status()
            data = res.json()
            if isinstance(data, dict) and data.get('error'):
                raise RuntimeError(f"Kraken GET {path} failed errors={data.get('error')}")
            return data

        raise RuntimeError(f"Kraken: échec après 3 tentatives sur {path}")

    def heartbeat(self) -> dict:
        return {
            'service': 'collector',
            'status': 'ready' if self.collector_enabled else 'disabled_external_ingest_mode',
            'last_tick_at': datetime.now(timezone.utc).isoformat(),
            'base_url': self.base_url,
            'pipeline_intervals': list(PIPELINE_INTERVALS),
            'kraken_collector_enabled': self.collector_enabled,
        }

    def _runtime_csv(self, key: str) -> list[str]:
        return [
            item.strip().upper()
            for item in str(self.runtime.get('market_data', self.runtime['kraken']).get(key, '')).split(',')
            if item.strip()
        ]

    @staticmethod
    def _asset_name(asset: str) -> str:
        value = str(asset or '').upper()
        if value in {'XBT', 'XXBT'}:
            return 'BTC'
        return value.lstrip('XZ')

    @staticmethod
    def _symbol_name(base_asset: str, quote_asset: str) -> str:
        return f"{base_asset}{quote_asset}".upper().replace('/', '')

    @staticmethod
    def _kraken_status_allowed(row_status: str, configured_status: str) -> bool:
        wanted = str(configured_status or '').upper()
        actual = str(row_status or 'online').lower()
        if wanted in {'', 'TRADING', 'ONLINE'}:
            return actual == 'online'
        return actual == wanted.lower()

    @staticmethod
    def _kraken_interval_minutes(interval: str) -> int:
        value = str(interval).strip().lower()
        if value.endswith('m'):
            return int(value[:-1])
        if value.endswith('h'):
            return int(value[:-1]) * 60
        if value.endswith('d'):
            return int(value[:-1]) * 1440
        if value.endswith('w'):
            return int(value[:-1]) * 10080
        return int(value)

    def discover_symbols(self, limit: int | None = None) -> list[str]:
        if not self.collector_enabled:
            symbols = self._stored_symbols(limit=limit)
            logger.info(
                "Kraken collector disabled. Using %d symbols from stored market_candles.",
                len(symbols),
            )
            return symbols

        info = self._get('/0/public/AssetPairs', {'assetVersion': 1})
        allowed_quotes = self._runtime_csv('kraken_quote_assets')
        excluded_bases = set(self._runtime_csv('kraken_excluded_base_assets'))
        status_name = self.runtime.get('market_data', self.runtime['kraken'])['kraken_symbol_status']
        max_symbols = int(limit or self.runtime.get('market_data', self.runtime['kraken'])['kraken_max_symbols'])

        symbols: list[str] = []
        for key, row in (info.get('result') or {}).items():
            base_asset = self._asset_name(str(row.get('base') or ''))
            quote_asset = self._asset_name(str(row.get('quote') or ''))
            if not base_asset or not quote_asset:
                wsname = str(row.get('wsname') or '')
                if '/' in wsname:
                    base_asset, quote_asset = [self._asset_name(part) for part in wsname.split('/', 1)]
            symbol = self._symbol_name(base_asset, quote_asset)
            if not self._kraken_status_allowed(str(row.get('status') or 'online'), status_name):
                continue
            if quote_asset not in allowed_quotes:
                continue
            if base_asset in excluded_bases:
                continue
            if str(key).endswith('.d') or row.get('darkpool') is True:
                continue
            symbols.append(symbol)

        symbols = sorted(set(symbols))[:max_symbols]
        logger.info(
            "Discovered %d symbols without liquidity filters. quotes=%s status=%s max_symbols=%s",
            len(symbols), allowed_quotes, status_name, max_symbols,
        )
        return symbols

    def fetch_klines(self, symbol: str, interval: str, limit: int, start_time: int | None = None) -> list[dict[str, Any]]:
        if not self.collector_enabled:
            return []
        if limit <= 0:
            return []
        interval_minutes = self._kraken_interval_minutes(interval)
        params: dict[str, Any] = {'pair': symbol.upper(), 'interval': interval_minutes}
        if start_time is not None:
            params['since'] = int(start_time) // 1000
        raw = self._get('/0/public/OHLC', params)
        result = raw.get('result') or {}
        rows: list[Any] = []
        for key, value in result.items():
            if key != 'last':
                rows = value or []
                break
        rows = rows[-limit:]
        width_ms = interval_minutes * 60 * 1000
        now_ms = int(time.time() * 1000)
        candles = []
        for r in rows:
            open_time = int(float(r[0]) * 1000)
            close_time = open_time + width_ms - 1
            close = float(r[4])
            volume = float(r[6])
            if close_time > now_ms:
                continue
            candles.append({
                'open_time': open_time,
                'open': float(r[1]),
                'high': float(r[2]),
                'low': float(r[3]),
                'close': close,
                'volume': volume,
                'close_time': close_time,
                'quote_volume': volume * close,
                'number_of_trades': int(r[7]),
                'taker_buy_base_volume': 0.0,
                'taker_buy_quote_volume': 0.0,
            })
        return candles

    def _full_lookback(self, interval: str) -> int:
        return int(self.runtime.get('market_data', self.runtime['kraken'])[f'kraken_lookback_{interval}'])

    def _incremental_min(self, interval: str) -> int:
        return int(self.runtime.get('market_data', self.runtime['kraken']).get(f'kraken_incremental_min_{interval}', 2))

    def _interval_due(self, interval: str, latest_close_time: int | None) -> bool:
        if interval not in DUE_BASED_INTERVALS:
            return True
        if latest_close_time is None:
            return True
        now_ms = int(time.time() * 1000)
        return now_ms >= int(latest_close_time) + INTERVAL_MS[interval]

    def _compute_incremental_limit(self, interval: str, latest_close_time: int | None) -> tuple[int, int | None]:
        full_limit = self._full_lookback(interval)
        if interval in DUE_BASED_INTERVALS and not self._interval_due(interval, latest_close_time):
            return 0, None
        if not self.runtime.get('market_data', self.runtime['kraken']).get('kraken_incremental_fetch_enabled', True) or latest_close_time is None:
            return full_limit, None
        interval_ms = INTERVAL_MS[interval]
        now_ms = int(time.time() * 1000)
        missed_bars = max(0, math.ceil((now_ms - int(latest_close_time)) / interval_ms))
        limit = min(full_limit, max(self._incremental_min(interval), missed_bars + 2))
        return int(limit), int(latest_close_time) + 1

    def collect_interval(self, symbol: str, interval: str, latest_close_time: int | None = None) -> list[dict[str, Any]]:
        if not self.collector_enabled:
            return []
        limit, start_time = self._compute_incremental_limit(interval, latest_close_time)
        return self.fetch_klines(symbol, interval, limit, start_time=start_time)

    def collect_symbol_bundle(self, symbol: str, latest_close_times: dict[str, int] | None = None) -> dict[str, list[dict[str, Any]]]:
        latest_close_times = latest_close_times or {}
        bundle: dict[str, list[dict[str, Any]]] = {}
        for interval in PIPELINE_INTERVALS:
            bundle[interval] = self.collect_interval(symbol, interval, latest_close_times.get(interval))
        return bundle

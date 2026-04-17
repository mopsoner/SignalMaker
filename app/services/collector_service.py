import logging
import math
import time
from datetime import datetime, timezone
from typing import Any

import requests

from app.services.runtime_settings import load_runtime_settings

logger = logging.getLogger(__name__)

BINANCE_WEIGHT_LIMIT = 1200
WEIGHT_SAFETY_THRESHOLD = 1000
KLINES_WEIGHT = 2
INTERVAL_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
}


class RateLimiter:
    """Tracks Binance rolling-minute weight and pauses when approaching the limit."""

    def __init__(self) -> None:
        self._used_weight: int = 0
        self._window_start: float = time.monotonic()

    def record(self, used_weight: int) -> None:
        now = time.monotonic()
        if now - self._window_start >= 60:
            self._used_weight = 0
            self._window_start = now
        self._used_weight = used_weight

    def wait_if_needed(self) -> None:
        if self._used_weight >= WEIGHT_SAFETY_THRESHOLD:
            elapsed = time.monotonic() - self._window_start
            wait = max(0.0, 61.0 - elapsed)
            if wait > 0:
                logger.warning(
                    "Binance weight %d/%d — pause %.1fs avant reset",
                    self._used_weight, BINANCE_WEIGHT_LIMIT, wait,
                )
                time.sleep(wait)
                self._used_weight = 0
                self._window_start = time.monotonic()


class CollectorService:
    def __init__(self) -> None:
        runtime = load_runtime_settings()
        self.runtime = runtime
        self.base_url = runtime['binance']['binance_rest_base'].rstrip('/')
        self.session = requests.Session()
        self._rate = RateLimiter()

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        self._rate.wait_if_needed()
        url = f"{self.base_url}{path}"
        for attempt in range(3):
            try:
                res = self.session.get(url, params=params or {}, timeout=15)
            except requests.RequestException:
                if attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                raise

            used = res.headers.get('X-MBX-USED-WEIGHT-1M')
            if used is not None:
                self._rate.record(int(used))

            if res.status_code == 418:
                retry_after = int(res.headers.get('Retry-After', 120))
                logger.error(
                    "Binance IP ban (418) — attente %ds avant reprise", retry_after
                )
                raise RuntimeError(
                    f"Binance a banni l'IP temporairement (418). "
                    f"Attendez {retry_after}s avant de relancer le pipeline."
                )

            if res.status_code == 429:
                retry_after = int(res.headers.get('Retry-After', 60))
                logger.warning(
                    "Binance rate-limit (429) — attente %ds (tentative %d/3)",
                    retry_after, attempt + 1,
                )
                time.sleep(retry_after)
                continue

            res.raise_for_status()
            return res.json()

        raise RuntimeError(f"Binance: échec après 3 tentatives sur {path}")

    def heartbeat(self) -> dict:
        return {
            'service': 'collector',
            'status': 'ready',
            'last_tick_at': datetime.now(timezone.utc).isoformat(),
            'base_url': self.base_url,
        }

    def discover_symbols(self, limit: int | None = None) -> list[str]:
        info = self._get('/api/v3/exchangeInfo')
        out: list[str] = []
        allowed_quotes = [item.strip().upper() for item in self.runtime['binance']['binance_quote_assets'].split(',') if item.strip()]
        status_name = self.runtime['binance']['binance_symbol_status']
        max_symbols = self.runtime['binance']['binance_max_symbols']
        for row in info.get('symbols', []):
            if row.get('status') != status_name:
                continue
            if row.get('quoteAsset') not in allowed_quotes:
                continue
            if not row.get('isSpotTradingAllowed', False):
                continue
            out.append(row['symbol'])
        out = sorted(set(out))
        return out[: (limit or max_symbols)]

    def fetch_klines(self, symbol: str, interval: str, limit: int, start_time: int | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {'symbol': symbol, 'interval': interval, 'limit': limit}
        if start_time is not None:
            params['startTime'] = start_time
        raw = self._get('/api/v3/klines', params)
        return [
            {
                'open_time': int(r[0]),
                'open': float(r[1]),
                'high': float(r[2]),
                'low': float(r[3]),
                'close': float(r[4]),
                'volume': float(r[5]),
                'close_time': int(r[6]),
            }
            for r in raw
        ]

    def _full_lookback(self, interval: str) -> int:
        return int(self.runtime['binance'][f'binance_lookback_{interval}'])

    def _incremental_min(self, interval: str) -> int:
        return int(self.runtime['binance'].get(f'binance_incremental_min_{interval}', 2))

    def _compute_incremental_limit(self, interval: str, latest_close_time: int | None) -> tuple[int, int | None]:
        full_limit = self._full_lookback(interval)
        if not self.runtime['binance'].get('binance_incremental_fetch_enabled', True) or latest_close_time is None:
            return full_limit, None
        interval_ms = INTERVAL_MS[interval]
        now_ms = int(time.time() * 1000)
        missed_bars = max(0, math.ceil((now_ms - int(latest_close_time)) / interval_ms))
        limit = min(full_limit, max(self._incremental_min(interval), missed_bars + 2))
        return int(limit), int(latest_close_time) + 1

    def collect_symbol_bundle(self, symbol: str, latest_close_times: dict[str, int] | None = None) -> dict[str, list[dict[str, Any]]]:
        latest_close_times = latest_close_times or {}
        bundle: dict[str, list[dict[str, Any]]] = {}
        for interval in ('1m', '5m', '1h', '4h'):
            limit, start_time = self._compute_incremental_limit(interval, latest_close_times.get(interval))
            bundle[interval] = self.fetch_klines(symbol, interval, limit, start_time=start_time)
        return bundle

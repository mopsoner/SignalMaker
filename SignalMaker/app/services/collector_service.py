import logging
import time
from datetime import datetime, timezone
from typing import Any

import requests

from app.core.config import settings

logger = logging.getLogger(__name__)

BINANCE_WEIGHT_LIMIT = 1200
WEIGHT_SAFETY_THRESHOLD = 1000


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
        self.base_url = settings.binance_rest_base.rstrip("/")
        self.session = requests.Session()
        self._rate = RateLimiter()

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        self._rate.wait_if_needed()
        url = f"{self.base_url}{path}"
        for attempt in range(3):
            try:
                res = self.session.get(url, params=params or {}, timeout=15)
            except requests.RequestException as exc:
                if attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                raise

            used = res.headers.get("X-MBX-USED-WEIGHT-1M")
            if used is not None:
                self._rate.record(int(used))

            if res.status_code == 418:
                retry_after = int(res.headers.get("Retry-After", 120))
                logger.error(
                    "Binance IP ban (418) — attente %ds avant reprise", retry_after
                )
                raise RuntimeError(
                    f"Binance a banni l'IP temporairement (418). "
                    f"Attendez {retry_after}s avant de relancer le pipeline."
                )

            if res.status_code == 429:
                retry_after = int(res.headers.get("Retry-After", 60))
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
            "service": "collector",
            "status": "ready",
            "last_tick_at": datetime.now(timezone.utc).isoformat(),
            "base_url": self.base_url,
        }

    def discover_symbols(self, limit: int | None = None) -> list[str]:
        info = self._get("/api/v3/exchangeInfo")
        out: list[str] = []
        for row in info.get("symbols", []):
            if row.get("status") != settings.binance_symbol_status:
                continue
            if row.get("quoteAsset") not in settings.quote_assets_list:
                continue
            if not row.get("isSpotTradingAllowed", False):
                continue
            out.append(row["symbol"])
        out = sorted(set(out))
        return out[: (limit or settings.binance_max_symbols)]

    def fetch_klines(self, symbol: str, interval: str, limit: int) -> list[dict[str, Any]]:
        raw = self._get("/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})
        return [
            {
                "open_time": int(r[0]),
                "open": float(r[1]),
                "high": float(r[2]),
                "low": float(r[3]),
                "close": float(r[4]),
                "volume": float(r[5]),
                "close_time": int(r[6]),
            }
            for r in raw
        ]

    def collect_symbol_bundle(self, symbol: str) -> dict[str, list[dict[str, Any]]]:
        return {
            "1m": self.fetch_klines(symbol, "1m", settings.binance_lookback_1m),
            "5m": self.fetch_klines(symbol, "5m", settings.binance_lookback_5m),
            "1h": self.fetch_klines(symbol, "1h", settings.binance_lookback_1h),
            "4h": self.fetch_klines(symbol, "4h", settings.binance_lookback_4h),
        }

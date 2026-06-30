import logging
from datetime import datetime, timezone

from sqlalchemy import select

from app.db.session import SessionLocal
from app.models.market_candle import MarketCandle

logger = logging.getLogger(__name__)

INTERVAL_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
}
PIPELINE_INTERVALS = ("15m", "1h", "4h")
DUE_BASED_INTERVALS = {"1h", "4h"}


class CollectorService:
    """Read-only market-data adapter for SignalMaker main.

    The main server no longer fetches candles or symbols from an exchange. Candles
    are expected to be pushed by the Raspberry Executor through the market-data
    ingestion API. Discovery therefore uses symbols already present in storage,
    and collection methods are intentionally no-ops.
    """

    def __init__(self) -> None:
        self.collector_enabled = False

    def _stored_symbols(self, limit: int | None = None) -> list[str]:
        db = SessionLocal()
        try:
            stmt = select(MarketCandle.symbol).distinct().order_by(MarketCandle.symbol)
            if limit:
                stmt = stmt.limit(limit)
            return [str(symbol).upper() for symbol in db.scalars(stmt).all()]
        finally:
            db.close()

    def heartbeat(self) -> dict:
        return {
            'service': 'collector',
            'status': 'external_ingest_only',
            'last_tick_at': datetime.now(timezone.utc).isoformat(),
            'pipeline_intervals': list(PIPELINE_INTERVALS),
            'external_ingest_only': True,
        }

    def discover_symbols(self, limit: int | None = None) -> list[str]:
        symbols = self._stored_symbols(limit=limit)
        logger.info(
            "External-ingest mode: using %d symbols from stored market_candles.",
            len(symbols),
        )
        return symbols

    def fetch_candles(self, symbol: str, interval: str, limit: int, start_time: int | None = None) -> list[dict]:
        logger.debug(
            "External-ingest mode: skipping remote candle fetch for %s %s.",
            symbol,
            interval,
        )
        return []

    def collect_interval(self, symbol: str, interval: str, latest_close_time: int | None = None) -> list[dict]:
        return []

    def collect_symbol_bundle(self, symbol: str, latest_close_times: dict[str, int] | None = None) -> dict[str, list[dict]]:
        return {interval: [] for interval in PIPELINE_INTERVALS}

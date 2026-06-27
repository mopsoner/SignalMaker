from dataclasses import dataclass

from raspberry_executor.env_store import read_env


@dataclass(frozen=True)
class Settings:
    signalmaker_base_url: str
    gateway_id: str
    poll_seconds: int
    dry_run: bool
    quote_assets: list[str]
    allowed_symbols: list[str]
    order_quote_amount: float
    max_candidate_age_seconds: int
    binance_base_url: str
    binance_api_key: str
    binance_secret_key: str
    ibkr_market_feed_enabled: bool
    ibkr_cp_base_url: str
    ibkr_cp_verify_ssl: bool
    ibkr_cp_timeout_seconds: int
    ibkr_market_feed_poll_seconds: int
    ibkr_market_feed_intervals: list[str]
    ibkr_market_feed_period: str
    ibkr_market_feed_bar: str
    ibkr_market_feed_source: str
    ibkr_market_feed_outside_rth: bool
    ibkr_market_feed_max_workers: int
    ibkr_market_feed_requests_per_minute: int
    ibkr_market_feed_limit: int
    ibkr_market_feed_queue_analysis: bool
    ibkr_market_feed_universes: list[str]
    ibkr_market_feed_asset_types: list[str]
    ibkr_market_feed_symbols: list[str]
    ibkr_contract_cache_path: str
    ibkr_market_feed_retry_queue_path: str
    signalmaker_stock_etf_ibkr_ingest_path: str
    signalmaker_stock_etf_assets_path: str


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _csv(value: str | None, *, upper: bool = True) -> list[str]:
    items = [item.strip() for item in (value or "").split(",") if item.strip()]
    return [item.upper() for item in items] if upper else items


def _int(values: dict[str, str], key: str, default: str) -> int:
    try:
        return int(values.get(key, default) or default)
    except Exception:
        return int(default)


def _float(values: dict[str, str], key: str, default: str) -> float:
    try:
        return float(values.get(key, default) or default)
    except Exception:
        return float(default)


def load_settings() -> Settings:
    # Use the persisted env store as the single source of truth.
    # Do not use os.getenv here: systemd/process env can keep stale values such as
    # DRY_RUN=true even after settings bootstrap restored .env to DRY_RUN=false.
    values = read_env()
    quote_assets = _csv(values.get("QUOTE_ASSETS", "USDC"))
    return Settings(
        signalmaker_base_url=str(values.get("SIGNALMAKER_BASE_URL", "")).rstrip("/"),
        gateway_id=str(values.get("GATEWAY_ID", "raspberry-fr-1")),
        poll_seconds=_int(values, "POLL_SECONDS", "15"),
        dry_run=_bool(values.get("DRY_RUN"), default=False),
        quote_assets=quote_assets,
        allowed_symbols=quote_assets,
        order_quote_amount=_float(values, "ORDER_QUOTE_AMOUNT", "20"),
        max_candidate_age_seconds=_int(values, "MAX_CANDIDATE_AGE_SECONDS", "900"),
        binance_base_url=str(values.get("BINANCE_BASE_URL", "https://api.binance.com")).rstrip("/"),
        binance_api_key=str(values.get("BINANCE_API_KEY", "")),
        binance_secret_key=str(values.get("BINANCE_SECRET_KEY", "")),
        ibkr_market_feed_enabled=_bool(values.get("IBKR_MARKET_FEED_ENABLED"), default=False),
        ibkr_cp_base_url=str(values.get("IBKR_CP_BASE_URL", "https://localhost:5000/v1/api")).rstrip("/"),
        ibkr_cp_verify_ssl=_bool(values.get("IBKR_CP_VERIFY_SSL"), default=False),
        ibkr_cp_timeout_seconds=_int(values, "IBKR_CP_TIMEOUT_SECONDS", "30"),
        ibkr_market_feed_poll_seconds=_int(values, "IBKR_MARKET_FEED_POLL_SECONDS", "3600"),
        ibkr_market_feed_intervals=_csv(values.get("IBKR_MARKET_FEED_INTERVALS", "1d"), upper=False),
        ibkr_market_feed_period=str(values.get("IBKR_MARKET_FEED_PERIOD", "2y")),
        ibkr_market_feed_bar=str(values.get("IBKR_MARKET_FEED_BAR", "1d")),
        ibkr_market_feed_source=str(values.get("IBKR_MARKET_FEED_SOURCE", "Last")),
        ibkr_market_feed_outside_rth=_bool(values.get("IBKR_MARKET_FEED_OUTSIDE_RTH"), default=False),
        ibkr_market_feed_max_workers=_int(values, "IBKR_MARKET_FEED_MAX_WORKERS", "1"),
        ibkr_market_feed_requests_per_minute=_int(values, "IBKR_MARKET_FEED_REQUESTS_PER_MINUTE", "20"),
        ibkr_market_feed_limit=_int(values, "IBKR_MARKET_FEED_LIMIT", "300"),
        ibkr_market_feed_queue_analysis=_bool(values.get("IBKR_MARKET_FEED_QUEUE_ANALYSIS"), default=False),
        ibkr_market_feed_universes=_csv(values.get("IBKR_MARKET_FEED_UNIVERSE", "ETF PEA,ETF Europe UCITS,Stocks Euronext Paris,Stocks Europe"), upper=False),
        ibkr_market_feed_asset_types=_csv(values.get("IBKR_MARKET_FEED_ASSET_TYPES", "ETF,STOCK")),
        ibkr_market_feed_symbols=_csv(values.get("IBKR_MARKET_FEED_SYMBOLS", "")),
        ibkr_contract_cache_path=str(values.get("IBKR_CONTRACT_CACHE_PATH", "raspberry_executor/ibkr_contract_cache.json")),
        ibkr_market_feed_retry_queue_path=str(values.get("IBKR_MARKET_FEED_RETRY_QUEUE_PATH", "raspberry_executor/ibkr_market_retry_queue.json")),
        signalmaker_stock_etf_ibkr_ingest_path=str(values.get("SIGNALMAKER_STOCK_ETF_IBKR_INGEST_PATH", "/api/v1/stocks-etfs/ibkr/candles")),
        signalmaker_stock_etf_assets_path=str(values.get("SIGNALMAKER_STOCK_ETF_ASSETS_PATH", "/api/v1/stocks-etfs/assets")),
    )

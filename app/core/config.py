from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Bootstrap settings loaded from environment at process start.

    Runtime/admin-managed values are stored in app_settings via
    app.services.runtime_settings. This class remains the bootstrap fallback for
    defaults needed before the database-backed runtime settings are available.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @field_validator("database_url", mode="before")
    @classmethod
    def fix_database_url(cls, v: str) -> str:
        if isinstance(v, str):
            if v.startswith("postgres://"):
                return "postgresql+psycopg://" + v[len("postgres://"):]
            if v.startswith("postgresql://"):
                return "postgresql+psycopg://" + v[len("postgresql://"):]
        return v

    @field_validator("signal_execution_interval", mode="before")
    @classmethod
    def validate_execution_interval(cls, v: str) -> str:
        return "15m"

    @field_validator("momentum_candidates_min_rr", mode="before")
    @classmethod
    def empty_momentum_min_rr_as_none(cls, v: str | None) -> str | None:
        if isinstance(v, str) and not v.strip():
            return None
        return v

    app_name: str = Field(default="SignalMaker Raspberry Executor", alias="APP_NAME")
    app_env: str = Field(default="development", alias="APP_ENV")
    app_host: str = Field(default="0.0.0.0", alias="APP_HOST")
    app_port: int = Field(default=8080, alias="APP_PORT")
    database_url: str = Field(default="sqlite:///./signalmaker.db", alias="DATABASE_URL")
    sql_echo: bool = Field(default=False, alias="SQL_ECHO")
    create_tables_on_boot: bool = Field(default=True, alias="CREATE_TABLES_ON_BOOT")
    cors_origins: str = Field(default="http://localhost:8080,http://127.0.0.1:8080", alias="CORS_ORIGINS")
    cors_origin_regex: str = Field(default="", alias="CORS_ORIGIN_REGEX")

    admin_token: str = Field(default="changeme-admin-token", alias="ADMIN_TOKEN")

    kraken_rest_base: str = Field(default="https://api.kraken.us", alias="KRAKEN_REST_BASE")
    kraken_collector_enabled: bool = Field(default=True, alias="KRAKEN_COLLECTOR_ENABLED")
    kraken_testnet_rest_base: str = Field(default="https://testnet.kraken.vision", alias="KRAKEN_TESTNET_REST_BASE")
    kraken_api_key: str = Field(default="", alias="KRAKEN_API_KEY")
    kraken_secret_key: str = Field(default="", alias="KRAKEN_SECRET_KEY")
    execution_exchange: str = Field(default="kraken", alias="EXECUTION_EXCHANGE")
    kraken_base_url: str = Field(default="https://api.kraken.com", alias="KRAKEN_BASE_URL")
    kraken_api_key: str = Field(default="", alias="KRAKEN_API_KEY")
    kraken_secret_key: str = Field(default="", alias="KRAKEN_SECRET_KEY")
    kraken_quote_assets: str = Field(default="USD,USDC", alias="KRAKEN_QUOTE_ASSETS")
    kraken_symbol_status: str = Field(default="TRADING", alias="KRAKEN_SYMBOL_STATUS")
    kraken_max_symbols: int = Field(default=25, alias="KRAKEN_MAX_SYMBOLS")
    kraken_min_quote_volume_24h: float = Field(default=1_000_000.0, alias="KRAKEN_MIN_QUOTE_VOLUME_24H")
    kraken_min_trades_24h: int = Field(default=1_000, alias="KRAKEN_MIN_TRADES_24H")
    kraken_excluded_base_assets: str = Field(default="USDT,USDC,FDUSD,TUSD,DAI,USDP,BUSD,EUR,GBP,USD,TRY,BRL", alias="KRAKEN_EXCLUDED_BASE_ASSETS")
    kraken_collect_max_workers: int = Field(default=4, alias="KRAKEN_COLLECT_MAX_WORKERS")
    kraken_incremental_fetch_enabled: bool = Field(default=True, alias="KRAKEN_INCREMENTAL_FETCH_ENABLED")
    kraken_incremental_min_1m: int = Field(default=3, alias="KRAKEN_INCREMENTAL_MIN_1M")
    kraken_incremental_min_5m: int = Field(default=3, alias="KRAKEN_INCREMENTAL_MIN_5M")
    kraken_incremental_min_15m: int = Field(default=3, alias="KRAKEN_INCREMENTAL_MIN_15M")
    kraken_incremental_min_1h: int = Field(default=2, alias="KRAKEN_INCREMENTAL_MIN_1H")
    kraken_incremental_min_4h: int = Field(default=2, alias="KRAKEN_INCREMENTAL_MIN_4H")
    kraken_lookback_1m: int = Field(default=180, alias="KRAKEN_LOOKBACK_1M")
    kraken_lookback_5m: int = Field(default=180, alias="KRAKEN_LOOKBACK_5M")
    kraken_lookback_15m: int = Field(default=180, alias="KRAKEN_LOOKBACK_15M")
    kraken_lookback_1h: int = Field(default=180, alias="KRAKEN_LOOKBACK_1H")
    kraken_lookback_4h: int = Field(default=120, alias="KRAKEN_LOOKBACK_4H")

    live_trading_enabled: bool = Field(default=False, alias="LIVE_TRADING_ENABLED")
    kraken_use_testnet: bool = Field(default=True, alias="KRAKEN_USE_TESTNET")
    live_spot_allow_shorts: bool = Field(default=False, alias="LIVE_SPOT_ALLOW_SHORTS")
    live_max_open_positions: int = Field(default=100, alias="LIVE_MAX_OPEN_POSITIONS")
    live_max_notional_per_trade: float = Field(default=250.0, alias="LIVE_MAX_NOTIONAL_PER_TRADE")
    live_require_tp_sl: bool = Field(default=True, alias="LIVE_REQUIRE_TP_SL")
    live_reconcile_enabled: bool = Field(default=True, alias="LIVE_RECONCILE_ENABLED")

    signalmaker_base_url: str = Field(default="https://mysginalmaker.replit.app", alias="SIGNALMAKER_BASE_URL")
    momentum_candidates_sync_enabled: bool = Field(default=False, alias="MOMENTUM_CANDIDATES_SYNC_ENABLED")
    momentum_candidates_limit: int = Field(default=100, alias="MOMENTUM_CANDIDATES_LIMIT")
    momentum_candidates_min_score: float = Field(default=0.0, alias="MOMENTUM_CANDIDATES_MIN_SCORE")
    momentum_candidates_min_rr: float | None = Field(default=None, alias="MOMENTUM_CANDIDATES_MIN_RR")
    momentum_candidates_require_wyckoff_context: bool = Field(default=True, alias="MOMENTUM_CANDIDATES_REQUIRE_WYCKOFF_CONTEXT")
    momentum_candidates_http_timeout_sec: float = Field(default=20.0, alias="MOMENTUM_CANDIDATES_HTTP_TIMEOUT_SEC")
    momentum_candidates_source_path: str = Field(default="/api/v1/momentum", alias="MOMENTUM_CANDIDATES_SOURCE_PATH")
    momentum_candidates_target_pct: float = Field(default=3.0, alias="MOMENTUM_CANDIDATES_TARGET_PCT")

    telegram_bot_token: str = Field(default="", alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: str = Field(default="", alias="TELEGRAM_CHAT_ID")
    discord_webhook_url: str = Field(default="", alias="DISCORD_WEBHOOK_URL")

    session_timezone_offset_hours: int = Field(default=-4, alias="SESSION_TIMEZONE_OFFSET_HOURS")
    signal_execution_interval: str = Field(default="15m", alias="SIGNAL_EXECUTION_INTERVAL")
    signal_rsi_period: int = Field(default=14, alias="SIGNAL_RSI_PERIOD")
    signal_swing_window: int = Field(default=8, alias="SIGNAL_SWING_WINDOW")
    signal_equal_level_tolerance_pct: float = Field(default=0.002, alias="SIGNAL_EQUAL_LEVEL_TOLERANCE_PCT")
    signal_overbought: float = Field(default=70, alias="SIGNAL_OVERBOUGHT")
    signal_oversold: float = Field(default=30, alias="SIGNAL_OVERSOLD")
    signal_price_near_extreme_pct: float = Field(default=0.0025, alias="SIGNAL_PRICE_NEAR_EXTREME_PCT")
    signal_session_confirm_filter_enabled: bool = Field(default=False, alias="SIGNAL_SESSION_CONFIRM_FILTER_ENABLED")

    planner_min_score: float = Field(default=4, alias="PLANNER_MIN_SCORE")
    planner_min_rr: float = Field(default=0.8, alias="PLANNER_MIN_RR")

    bot_pipeline_enabled: bool = Field(default=True, alias="BOT_PIPELINE_ENABLED")
    bot_executor_enabled: bool = Field(default=True, alias="BOT_EXECUTOR_ENABLED")
    bot_scheduler_enabled: bool = Field(default=True, alias="BOT_SCHEDULER_ENABLED")
    bot_pipeline_interval_sec: int = Field(default=60, alias="BOT_PIPELINE_INTERVAL_SEC")
    bot_executor_interval_sec: int = Field(default=30, alias="BOT_EXECUTOR_INTERVAL_SEC")
    bot_scheduler_interval_sec: int = Field(default=30, alias="BOT_SCHEDULER_INTERVAL_SEC")
    bot_executor_limit: int = Field(default=100, alias="BOT_EXECUTOR_LIMIT")
    bot_executor_quantity: float = Field(default=1.0, alias="BOT_EXECUTOR_QUANTITY")

    @property
    def cors_origin_list(self) -> list[str]:
        return [item.strip() for item in self.cors_origins.split(",") if item.strip()]

    @property
    def quote_assets_list(self) -> list[str]:
        return [item.strip().upper() for item in self.kraken_quote_assets.split(",") if item.strip()]

    def signal_config(self) -> dict:
        return {
            "execution_interval": "15m",
            "rsi_period": self.signal_rsi_period,
            "swing_window": self.signal_swing_window,
            "equal_level_tolerance_pct": self.signal_equal_level_tolerance_pct,
            "session_timezone_offset_hours": self.session_timezone_offset_hours,
            "session_confirm_filter_enabled": self.signal_session_confirm_filter_enabled,
            "signals": {
                "overbought": self.signal_overbought,
                "oversold": self.signal_oversold,
                "price_near_extreme_pct": self.signal_price_near_extreme_pct,
            },
        }


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

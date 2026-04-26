from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
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

    app_name: str = Field(default="SignalMaker", alias="APP_NAME")
    app_env: str = Field(default="development", alias="APP_ENV")
    app_host: str = Field(default="0.0.0.0", alias="APP_HOST")
    app_port: int = Field(default=8080, alias="APP_PORT")
    database_url: str = Field(default="sqlite:///./signalmaker.db", alias="DATABASE_URL")
    sql_echo: bool = Field(default=False, alias="SQL_ECHO")
    create_tables_on_boot: bool = Field(default=True, alias="CREATE_TABLES_ON_BOOT")
    cors_origins: str = Field(default="http://localhost:3000,http://localhost:8080", alias="CORS_ORIGINS")

    admin_token: str = Field(default="changeme-admin-token", alias="ADMIN_TOKEN")

    binance_rest_base: str = Field(default="https://api.binance.us", alias="BINANCE_REST_BASE")
    binance_testnet_rest_base: str = Field(default="https://testnet.binance.vision", alias="BINANCE_TESTNET_REST_BASE")
    binance_api_key: str = Field(default="", alias="BINANCE_API_KEY")
    binance_secret_key: str = Field(default="", alias="BINANCE_SECRET_KEY")
    binance_quote_assets: str = Field(default="USDT", alias="BINANCE_QUOTE_ASSETS")
    binance_symbol_status: str = Field(default="TRADING", alias="BINANCE_SYMBOL_STATUS")
    binance_max_symbols: int = Field(default=25, alias="BINANCE_MAX_SYMBOLS")
    binance_min_quote_volume_24h: float = Field(default=1_000_000.0, alias="BINANCE_MIN_QUOTE_VOLUME_24H")
    binance_min_trades_24h: int = Field(default=1_000, alias="BINANCE_MIN_TRADES_24H")
    binance_excluded_base_assets: str = Field(default="USDT,USDC,FDUSD,TUSD,DAI,USDP,BUSD,EUR,GBP,USD,TRY,BRL", alias="BINANCE_EXCLUDED_BASE_ASSETS")
    binance_collect_max_workers: int = Field(default=4, alias="BINANCE_COLLECT_MAX_WORKERS")
    binance_incremental_fetch_enabled: bool = Field(default=True, alias="BINANCE_INCREMENTAL_FETCH_ENABLED")
    binance_incremental_min_1m: int = Field(default=3, alias="BINANCE_INCREMENTAL_MIN_1M")
    binance_incremental_min_5m: int = Field(default=3, alias="BINANCE_INCREMENTAL_MIN_5M")
    binance_incremental_min_15m: int = Field(default=3, alias="BINANCE_INCREMENTAL_MIN_15M")
    binance_incremental_min_1h: int = Field(default=2, alias="BINANCE_INCREMENTAL_MIN_1H")
    binance_incremental_min_4h: int = Field(default=2, alias="BINANCE_INCREMENTAL_MIN_4H")
    binance_lookback_1m: int = Field(default=180, alias="BINANCE_LOOKBACK_1M")
    binance_lookback_5m: int = Field(default=180, alias="BINANCE_LOOKBACK_5M")
    binance_lookback_15m: int = Field(default=180, alias="BINANCE_LOOKBACK_15M")
    binance_lookback_1h: int = Field(default=180, alias="BINANCE_LOOKBACK_1H")
    binance_lookback_4h: int = Field(default=120, alias="BINANCE_LOOKBACK_4H")

    live_trading_enabled: bool = Field(default=False, alias="LIVE_TRADING_ENABLED")
    binance_use_testnet: bool = Field(default=True, alias="BINANCE_USE_TESTNET")
    live_spot_allow_shorts: bool = Field(default=False, alias="LIVE_SPOT_ALLOW_SHORTS")
    live_max_open_positions: int = Field(default=3, alias="LIVE_MAX_OPEN_POSITIONS")
    live_max_notional_per_trade: float = Field(default=250.0, alias="LIVE_MAX_NOTIONAL_PER_TRADE")
    live_require_tp_sl: bool = Field(default=True, alias="LIVE_REQUIRE_TP_SL")
    live_reconcile_enabled: bool = Field(default=True, alias="LIVE_RECONCILE_ENABLED")

    telegram_bot_token: str = Field(default="", alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: str = Field(default="", alias="TELEGRAM_CHAT_ID")
    discord_webhook_url: str = Field(default="", alias="DISCORD_WEBHOOK_URL")

    session_timezone_offset_hours: int = Field(default=-4, alias="SESSION_TIMEZONE_OFFSET_HOURS")
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
    bot_executor_limit: int = Field(default=10, alias="BOT_EXECUTOR_LIMIT")
    bot_executor_quantity: float = Field(default=1.0, alias="BOT_EXECUTOR_QUANTITY")

    @property
    def cors_origin_list(self) -> list[str]:
        return [item.strip() for item in self.cors_origins.split(",") if item.strip()]

    @property
    def quote_assets_list(self) -> list[str]:
        return [item.strip().upper() for item in self.binance_quote_assets.split(",") if item.strip()]

    def signal_config(self) -> dict:
        return {
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

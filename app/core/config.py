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

    @field_validator("signal_execution_interval", mode="before")
    @classmethod
    def validate_execution_interval(cls, v: str) -> str:
        value = str(v or "15m").strip().lower()
        return value if value in {"5m", "15m", "1h", "4h"} else "15m"

    @field_validator("signal_entry_rsi_timeframe", mode="before")
    @classmethod
    def validate_entry_rsi_timeframe(cls, v: str) -> str:
        value = str(v or "1h").strip().lower()
        return value if value in {"1h", "4h"} else "1h"

    app_name: str = Field(default="SignalMaker", alias="APP_NAME")
    app_env: str = Field(default="development", alias="APP_ENV")
    app_host: str = Field(default="0.0.0.0", alias="APP_HOST")
    app_port: int = Field(default=8080, alias="APP_PORT")
    database_url: str = Field(default="sqlite:///./signalmaker.db", alias="DATABASE_URL")
    sql_echo: bool = Field(default=False, alias="SQL_ECHO")
    create_tables_on_boot: bool = Field(default=True, alias="CREATE_TABLES_ON_BOOT")
    cors_origins: str = Field(default="http://localhost:3000,http://localhost:8080", alias="CORS_ORIGINS")

    admin_token: str = Field(default="changeme-admin-token", alias="ADMIN_TOKEN")

    kraken_execution_enabled: bool = Field(default=False, alias="KRAKEN_EXECUTION_ENABLED")
    kraken_dry_run: bool = Field(default=True, alias="KRAKEN_DRY_RUN")
    kraken_base_url: str = Field(default="https://api.kraken.com", alias="KRAKEN_BASE_URL")
    kraken_api_key: str = Field(default="", alias="KRAKEN_API_KEY")
    kraken_secret_key: str = Field(default="", alias="KRAKEN_SECRET_KEY")
    kraken_order_quote_amount: float = Field(default=50.0, alias="KRAKEN_ORDER_QUOTE_AMOUNT")
    kraken_quote_assets: str = Field(default="USD", alias="KRAKEN_QUOTE_ASSETS")
    kraken_min_buy_notional: float = Field(default=5.0, alias="KRAKEN_MIN_BUY_NOTIONAL")
    kraken_quote_reserve: float = Field(default=1.0, alias="KRAKEN_QUOTE_RESERVE")
    kraken_buy_balance_ratio: float = Field(default=0.995, alias="KRAKEN_BUY_BALANCE_RATIO")
    kraken_margin_execution_enabled: bool = Field(default=False, alias="KRAKEN_MARGIN_EXECUTION_ENABLED")
    kraken_margin_max_leverage: int = Field(default=10, alias="KRAKEN_MARGIN_MAX_LEVERAGE")
    kraken_margin_shorts_enabled: bool = Field(default=False, alias="KRAKEN_MARGIN_SHORTS_ENABLED")
    kraken_margin_allow_spot_fallback: bool = Field(default=False, alias="KRAKEN_MARGIN_ALLOW_SPOT_FALLBACK")
    momentum_execution_enabled: bool = Field(default=False, alias="MOMENTUM_EXECUTION_ENABLED")
    momentum_execution_mode: str = Field(default="margin", alias="MOMENTUM_EXECUTION_MODE")
    momentum_execution_balance_confirm_attempts: int = Field(default=8, alias="MOMENTUM_EXECUTION_BALANCE_CONFIRM_ATTEMPTS")
    momentum_execution_balance_confirm_sleep: float = Field(default=1.0, alias="MOMENTUM_EXECUTION_BALANCE_CONFIRM_SLEEP")

    live_spot_allow_shorts: bool = Field(default=False, alias="LIVE_SPOT_ALLOW_SHORTS")
    live_max_open_positions: int = Field(default=3, alias="LIVE_MAX_OPEN_POSITIONS")
    live_max_notional_per_trade: float = Field(default=250.0, alias="LIVE_MAX_NOTIONAL_PER_TRADE")
    live_require_tp_sl: bool = Field(default=True, alias="LIVE_REQUIRE_TP_SL")
    live_reconcile_enabled: bool = Field(default=True, alias="LIVE_RECONCILE_ENABLED")


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
    signal_entry_rsi_min: float = Field(default=50.0, alias="SIGNAL_ENTRY_RSI_MIN")
    signal_entry_rsi_max: float = Field(default=65.0, alias="SIGNAL_ENTRY_RSI_MAX")
    signal_entry_rsi_timeframe: str = Field(default="1h", alias="SIGNAL_ENTRY_RSI_TIMEFRAME")
    signal_price_near_extreme_pct: float = Field(default=0.0025, alias="SIGNAL_PRICE_NEAR_EXTREME_PCT")
    signal_session_confirm_filter_enabled: bool = Field(default=False, alias="SIGNAL_SESSION_CONFIRM_FILTER_ENABLED")

    planner_min_score: float = Field(default=25, alias="PLANNER_MIN_SCORE")
    planner_min_rr: float = Field(default=1.75, alias="PLANNER_MIN_RR")

    bot_pipeline_enabled: bool = Field(default=True, alias="BOT_PIPELINE_ENABLED")
    bot_executor_enabled: bool = Field(default=True, alias="BOT_EXECUTOR_ENABLED")
    bot_scheduler_enabled: bool = Field(default=True, alias="BOT_SCHEDULER_ENABLED")
    bot_pipeline_interval_sec: int = Field(default=900, alias="BOT_PIPELINE_INTERVAL_SEC")
    bot_executor_interval_sec: int = Field(default=30, alias="BOT_EXECUTOR_INTERVAL_SEC")
    bot_scheduler_interval_sec: int = Field(default=30, alias="BOT_SCHEDULER_INTERVAL_SEC")
    bot_momentum_engine_interval_sec: int = Field(default=300, alias="BOT_MOMENTUM_ENGINE_INTERVAL_SEC")
    momentum_engine_cadence_hours: int = Field(default=1, alias="MOMENTUM_ENGINE_CADENCE_HOURS")
    bot_executor_limit: int = Field(default=10, alias="BOT_EXECUTOR_LIMIT")
    bot_executor_quantity: float = Field(default=1.0, alias="BOT_EXECUTOR_QUANTITY")
    bot_momentum_engine_enabled: bool = Field(default=True, alias="BOT_MOMENTUM_ENGINE_ENABLED")

    momentum_engine_enabled: bool = Field(default=True, alias="MOMENTUM_ENGINE_ENABLED")
    momentum_engine_starting_capital: float = Field(default=1000.0, alias="MOMENTUM_ENGINE_STARTING_CAPITAL")
    momentum_engine_min_score: float = Field(default=0.0, alias="MOMENTUM_ENGINE_MIN_SCORE")

    stock_etf_feeder_enabled: bool = Field(default=False, alias="STOCK_ETF_FEEDER_ENABLED")
    stock_etf_momentum_enabled: bool = Field(default=False, alias="STOCK_ETF_MOMENTUM_ENABLED")
    stock_etf_wyckoff_smc_enabled: bool = Field(default=False, alias="STOCK_ETF_WYCKOFF_SMC_ENABLED")
    stock_etf_momentum_cadence_hours: int = Field(default=24, alias="STOCK_ETF_MOMENTUM_CADENCE_HOURS")
    stock_etf_wyckoff_smc_cadence_hours: int = Field(default=1, alias="STOCK_ETF_WYCKOFF_SMC_CADENCE_HOURS")
    stock_etf_exchange_timezone: str = Field(default="Europe/Paris", alias="STOCK_ETF_EXCHANGE_TIMEZONE")
    stock_etf_market_open: str = Field(default="09:00", alias="STOCK_ETF_MARKET_OPEN")
    stock_etf_market_close: str = Field(default="17:30", alias="STOCK_ETF_MARKET_CLOSE")
    stock_etf_retry_max_attempts: int = Field(default=3, alias="STOCK_ETF_RETRY_MAX_ATTEMPTS")
    stock_etf_retry_delay_seconds: int = Field(default=30, alias="STOCK_ETF_RETRY_DELAY_SECONDS")
    stock_etf_timeout_seconds: int = Field(default=1800, alias="STOCK_ETF_TIMEOUT_SECONDS")
    stock_etf_paper_starting_capital: float = Field(default=1000.0, alias="STOCK_ETF_PAPER_STARTING_CAPITAL")
    stock_etf_paper_max_positions: int = Field(default=1, alias="STOCK_ETF_PAPER_MAX_POSITIONS")
    stock_etf_paper_max_position_pct: float = Field(default=10.0, alias="STOCK_ETF_PAPER_MAX_POSITION_PCT")

    scheduler_reconciliation_interval_seconds: int = Field(default=300, alias="SCHEDULER_RECONCILIATION_INTERVAL_SECONDS")
    scheduler_abandoned_after_seconds: int = Field(default=900, alias="SCHEDULER_ABANDONED_AFTER_SECONDS")

    @property
    def cors_origin_list(self) -> list[str]:
        return [item.strip() for item in self.cors_origins.split(",") if item.strip()]


    def signal_config(self) -> dict:
        return {
            "execution_interval": self.signal_execution_interval,
            "rsi_period": self.signal_rsi_period,
            "swing_window": self.signal_swing_window,
            "equal_level_tolerance_pct": self.signal_equal_level_tolerance_pct,
            "session_timezone_offset_hours": self.session_timezone_offset_hours,
            "session_confirm_filter_enabled": self.signal_session_confirm_filter_enabled,
            "entry_rsi": {
                "min": self.signal_entry_rsi_min,
                "max": self.signal_entry_rsi_max,
                "timeframe": self.signal_entry_rsi_timeframe,
            },
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

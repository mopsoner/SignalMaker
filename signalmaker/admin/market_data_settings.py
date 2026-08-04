import os

from signalmaker.data_providers.ibkr.config import get_ibkr_config


def market_data_settings(repo, stock_etf: dict | None = None):
    ibkr = get_ibkr_config()
    return {
        "primary_provider": "IBKR",
        "ibkr_enabled": ibkr.enabled,
        "ibkr_auth_method": ibkr.auth_method,
        "ibkr_bearer_token_configured": bool(ibkr.bearer_token),
        "ibkr_oauth2_client_configured": bool(ibkr.oauth2_client_id and ibkr.oauth2_private_key),
        "default_timeframe": os.getenv("MARKET_DATA_DEFAULT_TIMEFRAME", ibkr.default_timeframe),
        "default_exchange": ibkr.default_exchange,
        "max_concurrent": ibkr.max_concurrent,
        "request_sleep_seconds": ibkr.request_sleep_seconds,
        "start_date": ibkr.start_date,
        "ibkr_history_period": ibkr.history_period,
        "ibkr_history_bar": ibkr.history_bar,
        "ibkr_use_rth": ibkr.use_regular_trading_hours,
        "stock_etf": stock_etf or {},
        **repo.stats(),
    }

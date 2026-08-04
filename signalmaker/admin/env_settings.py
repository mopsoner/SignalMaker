import os
from datetime import datetime

from signalmaker.data_providers.ibkr.config import get_ibkr_config

ENV_VARS = [
    "IBKR_ENABLED", "IBKR_AUTH_METHOD", "IBKR_BEARER_TOKEN", "IBKR_BASE_URL", "IBKR_TRADING_BASE_PATH",
    "IBKR_DEFAULT_EXCHANGE", "IBKR_DEFAULT_TIMEFRAME", "IBKR_REQUEST_SLEEP_SECONDS", "IBKR_MAX_CONCURRENT",
    "IBKR_START_DATE", "IBKR_HISTORY_PERIOD", "IBKR_HISTORY_BAR", "IBKR_USE_RTH", "IBKR_OAUTH2_TOKEN_URL",
    "IBKR_OAUTH2_CLIENT_ID", "IBKR_OAUTH2_PRIVATE_KEY", "IBKR_OAUTH2_PRIVATE_KEY_FILE", "IBKR_OAUTH2_KEY_ID",
    "IBKR_OAUTH2_SCOPE", "IBKR_OAUTH2_GRANT_TYPE", "IBKR_OAUTH2_JWT_ALGORITHM", "IBKR_OAUTH2_ASSERTION_TTL_SECONDS",
    "MARKET_DATA_DEFAULT_TIMEFRAME", "MARKET_DATA_ENABLE_STOCKS", "MARKET_DATA_ENABLE_ETFS",
    "MARKET_DATA_ENABLE_INDICES", "ADMIN_ENV_SETTINGS_ENABLED",
]
SECRETS = {"IBKR_BEARER_TOKEN", "IBKR_OAUTH2_PRIVATE_KEY"}


def env_status():
    ibkr = get_ibkr_config()
    warnings = []
    if ibkr.enabled and ibkr.auth_method == "bearer" and not ibkr.bearer_token:
        warnings.append("IBKR_AUTH_METHOD=bearer but IBKR_BEARER_TOKEN is missing")
    if ibkr.enabled and ibkr.auth_method == "oauth2_private_key_jwt" and (not ibkr.oauth2_client_id or not ibkr.oauth2_private_key):
        warnings.append("IBKR_AUTH_METHOD=oauth2_private_key_jwt requires IBKR_OAUTH2_CLIENT_ID and a private key")
    if ibkr.auth_method not in {"gateway", "bearer", "oauth2_private_key_jwt"}:
        warnings.append("IBKR_AUTH_METHOD must be gateway, bearer, or oauth2_private_key_jwt")
    if not ibkr.enabled:
        warnings.append("IBKR_ENABLED=false")
    if ibkr.max_concurrent > 10:
        warnings.append("IBKR_MAX_CONCURRENT may be too high")
    try:
        datetime.strptime(ibkr.start_date, "%Y-%m-%d")
    except ValueError:
        warnings.append("IBKR_START_DATE must use YYYY-MM-DD")
    if os.getenv("ADMIN_ENV_SETTINGS_ENABLED", "true").lower() == "false":
        warnings.append("ADMIN_ENV_SETTINGS_ENABLED=false")
    worker_control_supported = os.getenv("WORKER_CONTROL_ENABLED", "false").lower() in {"1", "true", "yes"}
    return {"variables": [{"name": key, "configured": bool(os.getenv(key)), "value": "***" if key in SECRETS and os.getenv(key) else os.getenv(key, ""), "secret": key in SECRETS} for key in ENV_VARS], "warnings": warnings, "editing_supported": False, "worker_control_supported": worker_control_supported, "instructions": "Update secrets in Replit Secrets or deployment environment; runtime writes are intentionally not performed."}

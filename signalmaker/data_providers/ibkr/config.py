from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class IBKRConfig:
    enabled: bool
    auth_method: str
    bearer_token: str
    base_url: str
    trading_base_path: str
    default_exchange: str
    default_timeframe: str
    request_sleep_seconds: float
    max_concurrent: int
    start_date: str
    history_period: str
    history_bar: str
    use_regular_trading_hours: bool
    oauth2_token_url: str
    oauth2_client_id: str
    oauth2_private_key: str
    oauth2_key_id: str
    oauth2_scope: str
    oauth2_grant_type: str
    oauth2_jwt_algorithm: str
    oauth2_assertion_ttl_seconds: int


def _private_key_from_env() -> str:
    raw_key = os.getenv("IBKR_OAUTH2_PRIVATE_KEY", "")
    if raw_key:
        return raw_key.replace("\\n", "\n")
    key_file = os.getenv("IBKR_OAUTH2_PRIVATE_KEY_FILE", "")
    if key_file:
        return Path(key_file).read_text()
    return ""


def get_ibkr_config() -> IBKRConfig:
    base_url = os.getenv("IBKR_BASE_URL", "https://api.ibkr.com").rstrip("/")
    return IBKRConfig(
        enabled=os.getenv("IBKR_ENABLED", "false").lower() == "true",
        auth_method=os.getenv("IBKR_AUTH_METHOD", "gateway").lower(),
        bearer_token=os.getenv("IBKR_BEARER_TOKEN", ""),
        base_url=base_url,
        trading_base_path=os.getenv("IBKR_TRADING_BASE_PATH", "/v1/api"),
        default_exchange=os.getenv("IBKR_DEFAULT_EXCHANGE", "SMART"),
        default_timeframe=os.getenv("IBKR_DEFAULT_TIMEFRAME", "1d"),
        request_sleep_seconds=float(os.getenv("IBKR_REQUEST_SLEEP_SECONDS", "1")),
        max_concurrent=int(os.getenv("IBKR_MAX_CONCURRENT", "2")),
        start_date=os.getenv("IBKR_START_DATE", "2020-01-01"),
        history_period=os.getenv("IBKR_HISTORY_PERIOD", "5y"),
        history_bar=os.getenv("IBKR_HISTORY_BAR", "1d"),
        use_regular_trading_hours=os.getenv("IBKR_USE_RTH", "true").lower() == "true",
        oauth2_token_url=os.getenv("IBKR_OAUTH2_TOKEN_URL", f"{base_url}/oauth2/api/v1/token"),
        oauth2_client_id=os.getenv("IBKR_OAUTH2_CLIENT_ID", ""),
        oauth2_private_key=_private_key_from_env(),
        oauth2_key_id=os.getenv("IBKR_OAUTH2_KEY_ID", ""),
        oauth2_scope=os.getenv("IBKR_OAUTH2_SCOPE", ""),
        oauth2_grant_type=os.getenv("IBKR_OAUTH2_GRANT_TYPE", "client_credentials"),
        oauth2_jwt_algorithm=os.getenv("IBKR_OAUTH2_JWT_ALGORITHM", "RS256"),
        oauth2_assertion_ttl_seconds=int(os.getenv("IBKR_OAUTH2_ASSERTION_TTL_SECONDS", "300")),
    )

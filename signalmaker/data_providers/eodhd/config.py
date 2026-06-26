from dataclasses import dataclass
import os


@dataclass(frozen=True)
class EODHDConfig:
    enabled: bool
    api_key: str
    base_url: str
    default_exchange: str
    default_timeframe: str
    request_sleep_seconds: float
    max_concurrent: int
    adjusted_data: bool
    start_date: str


def get_eodhd_config() -> EODHDConfig:
    return EODHDConfig(
        enabled=os.getenv("EODHD_ENABLED", "false").lower() == "true",
        api_key=os.getenv("EODHD_API_KEY", ""),
        base_url=os.getenv("EODHD_BASE_URL", "https://eodhd.com/api"),
        default_exchange=os.getenv("EODHD_DEFAULT_EXCHANGE", "PA"),
        default_timeframe=os.getenv("EODHD_DEFAULT_TIMEFRAME", "1d"),
        request_sleep_seconds=float(os.getenv("EODHD_REQUEST_SLEEP_SECONDS", "1")),
        max_concurrent=int(os.getenv("EODHD_MAX_CONCURRENT", "3")),
        adjusted_data=os.getenv("EODHD_ADJUSTED_DATA", "true").lower() == "true",
        start_date=os.getenv("EODHD_START_DATE", "2020-01-01"),
    )

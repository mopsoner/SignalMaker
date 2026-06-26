from dataclasses import dataclass
import os


@dataclass(frozen=True)
class IBKRConfig:
    enabled: bool
    host: str
    port: int
    client_id: int
    max_concurrent: int
    sleep_seconds: int
    duration: str
    bar_size: str
    use_rth: bool
    what_to_show: str


def get_ibkr_config() -> IBKRConfig:
    return IBKRConfig(
        enabled=os.getenv("IBKR_ENABLED", "false").lower() == "true",
        host=os.getenv("IBKR_HOST", "127.0.0.1"),
        port=int(os.getenv("IBKR_PORT", "4002")),
        client_id=int(os.getenv("IBKR_CLIENT_ID", "21")),
        max_concurrent=int(os.getenv("IBKR_HISTORICAL_MAX_CONCURRENT", "2")),
        sleep_seconds=int(os.getenv("IBKR_HISTORICAL_SLEEP_SECONDS", "12")),
        duration=os.getenv("IBKR_HISTORICAL_DURATION", "2 Y"),
        bar_size=os.getenv("IBKR_HISTORICAL_BAR_SIZE", "1 day"),
        use_rth=os.getenv("IBKR_HISTORICAL_USE_RTH", "true").lower() == "true",
        what_to_show=os.getenv("IBKR_HISTORICAL_WHAT_TO_SHOW", "TRADES"),
    )

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    signalmaker_base_url: str
    gateway_id: str
    poll_seconds: int
    dry_run: bool
    execution_quote_asset: str
    allowed_symbols: list[str]
    allow_shorts: bool
    order_quote_amount: float
    max_candidate_age_seconds: int
    binance_base_url: str
    binance_api_key: str
    binance_secret_key: str


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _csv(value: str | None) -> list[str]:
    return [item.strip().upper() for item in (value or "").split(",") if item.strip()]


def load_settings() -> Settings:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except Exception:
        pass

    return Settings(
        signalmaker_base_url=os.environ["SIGNALMAKER_BASE_URL"].rstrip("/"),
        gateway_id=os.getenv("GATEWAY_ID", "raspberry-fr-1"),
        poll_seconds=int(os.getenv("POLL_SECONDS", "15")),
        dry_run=_bool(os.getenv("DRY_RUN"), default=True),
        execution_quote_asset=os.getenv("EXECUTION_QUOTE_ASSET", "USDC").strip().upper(),
        allowed_symbols=_csv(os.getenv("ALLOWED_SYMBOLS", "")),
        allow_shorts=_bool(os.getenv("ALLOW_SHORTS"), default=False),
        order_quote_amount=float(os.getenv("ORDER_QUOTE_AMOUNT", "20")),
        max_candidate_age_seconds=int(os.getenv("MAX_CANDIDATE_AGE_SECONDS", "900")),
        binance_base_url=os.getenv("BINANCE_BASE_URL", "https://api.binance.com").rstrip("/"),
        binance_api_key=os.getenv("BINANCE_API_KEY", ""),
        binance_secret_key=os.getenv("BINANCE_SECRET_KEY", ""),
    )

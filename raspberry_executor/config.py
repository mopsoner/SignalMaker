import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    signalmaker_base_url: str
    gateway_id: str
    poll_seconds: int
    dry_run: bool
    allowed_symbols: list[str]
    symbol_map: dict[str, str]
    quantity: float
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


def _symbol_map(value: str | None) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for part in (value or "").split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            mapping[part.upper()] = part.upper()
            continue
        source, target = part.split(":", 1)
        mapping[source.strip().upper()] = target.strip().upper()
    return mapping


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
        allowed_symbols=_csv(os.getenv("ALLOWED_SYMBOLS", "ETHUSDT,BTCUSDT")),
        symbol_map=_symbol_map(os.getenv("SYMBOL_MAP", "ETHUSDT:ETHUSDC,BTCUSDT:BTCUSDC")),
        quantity=float(os.getenv("ORDER_QUANTITY", "0.001")),
        max_candidate_age_seconds=int(os.getenv("MAX_CANDIDATE_AGE_SECONDS", "900")),
        binance_base_url=os.getenv("BINANCE_BASE_URL", "https://api.binance.com").rstrip("/"),
        binance_api_key=os.getenv("BINANCE_API_KEY", ""),
        binance_secret_key=os.getenv("BINANCE_SECRET_KEY", ""),
    )

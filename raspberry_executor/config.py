from dataclasses import dataclass

from raspberry_executor.env_store import read_env


@dataclass(frozen=True)
class Settings:
    signalmaker_base_url: str
    gateway_id: str
    poll_seconds: int
    dry_run: bool
    quote_assets: list[str]
    allowed_symbols: list[str]
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


def _int(values: dict[str, str], key: str, default: str) -> int:
    try:
        return int(values.get(key, default) or default)
    except Exception:
        return int(default)


def _float(values: dict[str, str], key: str, default: str) -> float:
    try:
        return float(values.get(key, default) or default)
    except Exception:
        return float(default)


def load_settings() -> Settings:
    # Use the persisted env store as the single source of truth.
    # Do not use os.getenv here: systemd/process env can keep stale values such as
    # DRY_RUN=true even after settings bootstrap restored .env to DRY_RUN=false.
    values = read_env()
    quote_assets = _csv(values.get("QUOTE_ASSETS", "USDC"))
    return Settings(
        signalmaker_base_url=str(values.get("SIGNALMAKER_BASE_URL", "")).rstrip("/"),
        gateway_id=str(values.get("GATEWAY_ID", "raspberry-fr-1")),
        poll_seconds=_int(values, "POLL_SECONDS", "15"),
        dry_run=_bool(values.get("DRY_RUN"), default=False),
        quote_assets=quote_assets,
        allowed_symbols=quote_assets,
        order_quote_amount=_float(values, "ORDER_QUOTE_AMOUNT", "20"),
        max_candidate_age_seconds=_int(values, "MAX_CANDIDATE_AGE_SECONDS", "900"),
        binance_base_url=str(values.get("BINANCE_BASE_URL", "https://api.binance.com")).rstrip("/"),
        binance_api_key=str(values.get("BINANCE_API_KEY", "")),
        binance_secret_key=str(values.get("BINANCE_SECRET_KEY", "")),
    )

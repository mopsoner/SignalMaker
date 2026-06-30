from dataclasses import dataclass

from raspberry_executor.env_store import read_env


@dataclass(frozen=True, init=False)
class Settings:
    signalmaker_base_url: str
    gateway_id: str
    poll_seconds: int
    dry_run: bool
    quote_assets: list[str]
    allowed_symbols: list[str]
    order_quote_amount: float
    max_candidate_age_seconds: int
    kraken_base_url: str
    kraken_api_key: str
    kraken_secret_key: str
    exchange: str

    def __init__(self, **values):
        fields = self.__dataclass_fields__
        for name in fields:
            if name in values:
                object.__setattr__(self, name, values[name])
            else:
                raise TypeError(f"Settings missing required field: {name}")


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _csv(value: str | None, *, upper: bool = True) -> list[str]:
    items = [item.strip() for item in (value or "").split(",") if item.strip()]
    return [item.upper() for item in items] if upper else items


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


def _runtime_overrides() -> dict[str, str]:
    try:
        from app.services.runtime_settings import load_runtime_settings

        runtime = load_runtime_settings()
    except Exception:
        return {}
    overrides: dict[str, str] = {}
    kraken = runtime.get("kraken", {}) if isinstance(runtime.get("kraken"), dict) else {}
    executor = runtime.get("executor", {}) if isinstance(runtime.get("executor"), dict) else {}
    if kraken.get("kraken_base_url"):
        overrides["KRAKEN_BASE_URL"] = str(kraken["kraken_base_url"])
    if kraken.get("kraken_api_key"):
        overrides["KRAKEN_API_KEY"] = str(kraken["kraken_api_key"])
    if kraken.get("kraken_secret_key"):
        overrides["KRAKEN_SECRET_KEY"] = str(kraken["kraken_secret_key"])
    if executor.get("execution_exchange"):
        overrides["EXECUTION_EXCHANGE"] = str(executor["execution_exchange"])
    if executor.get("quote_assets"):
        value = executor["quote_assets"]
        overrides["QUOTE_ASSETS"] = ",".join(str(item) for item in value) if isinstance(value, list) else str(value)
    return overrides


def load_settings() -> Settings:
    # Runtime DB settings are the source of truth for admin-managed values.
    # The persisted env store remains the fallback when DB values are empty or unavailable.
    values = {**read_env(), **_runtime_overrides()}
    quote_assets = _csv(values.get("QUOTE_ASSETS", "USD,USDC"))
    return Settings(
        signalmaker_base_url=str(values.get("SIGNALMAKER_BASE_URL", "")).rstrip("/"),
        gateway_id=str(values.get("GATEWAY_ID", "raspberry-fr-1")),
        poll_seconds=_int(values, "POLL_SECONDS", "15"),
        dry_run=_bool(values.get("DRY_RUN"), default=False),
        quote_assets=quote_assets,
        allowed_symbols=quote_assets,
        order_quote_amount=_float(values, "ORDER_QUOTE_AMOUNT", "20"),
        max_candidate_age_seconds=_int(values, "MAX_CANDIDATE_AGE_SECONDS", "900"),
        kraken_base_url=str(values.get("KRAKEN_BASE_URL", "https://api.kraken.com")).rstrip("/"),
        kraken_api_key=str(values.get("KRAKEN_API_KEY", "")),
        kraken_secret_key=str(values.get("KRAKEN_SECRET_KEY", "")),
        exchange=str(values.get("EXECUTION_EXCHANGE", "kraken") or "kraken").strip().lower(),
    )

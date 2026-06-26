from dataclasses import dataclass
import os
from typing import Any


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


def _runtime_ibkr_settings() -> dict[str, Any]:
    try:
        from app.services.runtime_settings import load_runtime_settings

        return load_runtime_settings().get("ibkr", {})
    except Exception:
        return {}


def _get_bool(runtime: dict[str, Any], key: str, env_key: str, default: str) -> bool:
    if key in runtime:
        value = runtime[key]
        if isinstance(value, bool):
            return value
        return str(value).lower() == "true"
    return os.getenv(env_key, default).lower() == "true"


def _get_int(runtime: dict[str, Any], key: str, env_key: str, default: str) -> int:
    return int(runtime.get(key, os.getenv(env_key, default)))


def _get_str(runtime: dict[str, Any], key: str, env_key: str, default: str) -> str:
    return str(runtime.get(key, os.getenv(env_key, default)))


def get_ibkr_config() -> IBKRConfig:
    runtime = _runtime_ibkr_settings()
    return IBKRConfig(
        enabled=_get_bool(runtime, "ibkr_enabled", "IBKR_ENABLED", "false"),
        host=_get_str(runtime, "ibkr_host", "IBKR_HOST", "127.0.0.1"),
        port=_get_int(runtime, "ibkr_port", "IBKR_PORT", "4002"),
        client_id=_get_int(runtime, "ibkr_client_id", "IBKR_CLIENT_ID", "21"),
        max_concurrent=_get_int(runtime, "ibkr_historical_max_concurrent", "IBKR_HISTORICAL_MAX_CONCURRENT", "2"),
        sleep_seconds=_get_int(runtime, "ibkr_historical_sleep_seconds", "IBKR_HISTORICAL_SLEEP_SECONDS", "12"),
        duration=_get_str(runtime, "ibkr_historical_duration", "IBKR_HISTORICAL_DURATION", "2 Y"),
        bar_size=_get_str(runtime, "ibkr_historical_bar_size", "IBKR_HISTORICAL_BAR_SIZE", "1 day"),
        use_rth=_get_bool(runtime, "ibkr_historical_use_rth", "IBKR_HISTORICAL_USE_RTH", "true"),
        what_to_show=_get_str(runtime, "ibkr_historical_what_to_show", "IBKR_HISTORICAL_WHAT_TO_SHOW", "TRADES"),
    )

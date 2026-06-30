import os
from typing import Any

import requests


def _bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _secret_present(value: Any) -> bool:
    if value in (None, ""):
        return False
    return str(value).strip() not in {"********", "******", "***"}


def _settings_url(base_url: str) -> str:
    base = str(base_url or "").rstrip("/")
    if not base:
        return ""
    if base.endswith("/api/v1"):
        return f"{base}/admin/settings"
    return f"{base}/api/v1/admin/settings"


def _local_runtime_settings() -> tuple[dict, str | None]:
    try:
        from app.services.runtime_settings import load_runtime_settings

        payload = load_runtime_settings()
        return payload if isinstance(payload, dict) else {}, None
    except Exception as exc:
        return {}, str(exc)


def _merge_sections(primary: dict, fallback: dict) -> dict:
    merged = dict(fallback)
    for section, values in primary.items():
        if isinstance(values, dict) and isinstance(merged.get(section), dict):
            merged[section] = {**merged[section], **{k: v for k, v in values.items() if v not in (None, "")}}
        else:
            merged[section] = values
    return merged


def apply_admin_settings_to_environ(base_url: str | None = None, timeout: float = 5.0) -> dict:
    """Mirror runtime settings into process env with local DB priority.

    Local DB runtime settings are authoritative on the Raspberry. The Admin API
    is still queried as a fallback/diagnostic source for older deployments.
    """
    local_payload, local_error = _local_runtime_settings()
    payload: dict = local_payload.copy()
    admin_error = None
    admin_checked = False
    url = _settings_url(base_url or os.getenv("SIGNALMAKER_BASE_URL", ""))
    if url:
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            remote_payload = response.json()
            admin_checked = True
            if isinstance(remote_payload, dict):
                payload = _merge_sections(local_payload, remote_payload)
            else:
                admin_error = "invalid_admin_settings_payload"
        except Exception as exc:
            admin_error = str(exc)
    elif not local_payload:
        return {"applied": False, "reason": "missing_signalmaker_base_url", "local_runtime_error": local_error}
    if not isinstance(payload, dict):
        return {"applied": False, "reason": "invalid_admin_settings_payload", "local_runtime_error": local_error, "admin_settings_error": admin_error}

    live = payload.get("live") or {}
    executor = payload.get("executor") or {}
    kraken = payload.get("kraken") or {}
    kraken = payload.get("kraken") or {}
    market_data = payload.get("market_data") or {}

    if "live_trading_enabled" in live:
        dry_value = "false" if _bool(live.get("live_trading_enabled"), default=False) else "true"
        os.environ["DRY_RUN"] = dry_value
        os.environ["MARGIN_DRY_RUN"] = dry_value

    if "live_spot_allow_shorts" in live:
        os.environ["SHORTS_ENABLED"] = "true" if _bool(live.get("live_spot_allow_shorts"), default=False) else "false"

    if live.get("live_max_notional_per_trade") not in (None, ""):
        os.environ["ORDER_QUOTE_AMOUNT"] = str(live.get("live_max_notional_per_trade"))

    quote_assets = executor.get("quote_assets") or executor.get("QUOTE_ASSETS") or market_data.get("kraken_quote_assets") or kraken.get("kraken_quote_assets")
    if quote_assets:
        os.environ["QUOTE_ASSETS"] = str(quote_assets)

    execution_exchange = executor.get("execution_exchange") or executor.get("EXECUTION_EXCHANGE") or kraken.get("execution_exchange") or kraken.get("EXECUTION_EXCHANGE")
    if execution_exchange:
        os.environ["EXECUTION_EXCHANGE"] = str(execution_exchange).strip().lower()
    if kraken.get("kraken_base_url") or kraken.get("KRAKEN_BASE_URL"):
        os.environ["KRAKEN_BASE_URL"] = str(kraken.get("kraken_base_url") or kraken.get("KRAKEN_BASE_URL")).rstrip("/")
    kraken_api_key = kraken.get("kraken_api_key") or kraken.get("KRAKEN_API_KEY")
    kraken_secret_key = kraken.get("kraken_secret_key") or kraken.get("KRAKEN_SECRET_KEY")
    if _secret_present(kraken_api_key):
        os.environ["KRAKEN_API_KEY"] = str(kraken_api_key)
    if _secret_present(kraken_secret_key):
        os.environ["KRAKEN_SECRET_KEY"] = str(kraken_secret_key)

    if _bool(live.get("kraken_use_testnet"), default=False) and live.get("kraken_testnet_rest_base"):
        os.environ["KRAKEN_BASE_URL"] = str(live.get("kraken_testnet_rest_base")).rstrip("/")
    elif kraken.get("kraken_rest_base"):
        os.environ["KRAKEN_BASE_URL"] = str(kraken.get("kraken_rest_base")).rstrip("/")

    os.environ.setdefault("EXECUTION_MODE", "cross")
    os.environ.setdefault("MARGIN_MODE_ENABLED", "true")
    os.environ.setdefault("MARGIN_ACCOUNT_MODE", "cross")
    os.environ.setdefault("MARGIN_ISOLATED", "false")

    return {
        "applied": True,
        "dry_run": os.environ.get("DRY_RUN"),
        "margin_dry_run": os.environ.get("MARGIN_DRY_RUN"),
        "quote_assets": os.environ.get("QUOTE_ASSETS"),
        "order_quote_amount": os.environ.get("ORDER_QUOTE_AMOUNT"),
        "shorts_enabled": os.environ.get("SHORTS_ENABLED"),
        "kraken_base_url": os.environ.get("KRAKEN_BASE_URL"),
        "execution_exchange": os.environ.get("EXECUTION_EXCHANGE"),
        "kraken_base_url": os.environ.get("KRAKEN_BASE_URL"),
        "kraken_api_key_in_admin_payload": bool(kraken_api_key),
        "kraken_secret_key_in_admin_payload": bool(kraken_secret_key),
        "kraken_api_key_applied_to_env": _secret_present(kraken_api_key),
        "kraken_secret_key_applied_to_env": _secret_present(kraken_secret_key),
        "selected_source": "local_runtime_db" if local_payload else "admin_api",
        "admin_settings_checked": admin_checked,
        "admin_settings_error": admin_error,
        "local_runtime_error": local_error,
    }

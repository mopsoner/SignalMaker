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


def _settings_url(base_url: str) -> str:
    base = str(base_url or "").rstrip("/")
    if not base:
        return ""
    if base.endswith("/api/v1"):
        return f"{base}/admin/settings"
    return f"{base}/api/v1/admin/settings"


def apply_admin_settings_to_environ(base_url: str | None = None, timeout: float = 5.0) -> dict:
    """Mirror existing SignalMaker Admin settings into process env.

    This keeps the existing Admin fields as the source of truth. It is best
    effort: when the Admin API is unavailable, local .env remains the fallback.
    """
    url = _settings_url(base_url or os.getenv("SIGNALMAKER_BASE_URL", ""))
    if not url:
        return {"applied": False, "reason": "missing_signalmaker_base_url"}
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        return {"applied": False, "reason": "admin_settings_unavailable", "error": str(exc)}
    if not isinstance(payload, dict):
        return {"applied": False, "reason": "invalid_admin_settings_payload"}

    live = payload.get("live") or {}
    binance = payload.get("binance") or {}

    if "live_trading_enabled" in live:
        dry_value = "false" if _bool(live.get("live_trading_enabled"), default=False) else "true"
        os.environ["DRY_RUN"] = dry_value
        os.environ["MARGIN_DRY_RUN"] = dry_value

    if "live_spot_allow_shorts" in live:
        os.environ["SHORTS_ENABLED"] = "true" if _bool(live.get("live_spot_allow_shorts"), default=False) else "false"

    if live.get("live_max_notional_per_trade") not in (None, ""):
        os.environ["ORDER_QUOTE_AMOUNT"] = str(live.get("live_max_notional_per_trade"))

    if binance.get("binance_quote_assets"):
        os.environ["QUOTE_ASSETS"] = str(binance.get("binance_quote_assets"))

    if _bool(live.get("binance_use_testnet"), default=False) and live.get("binance_testnet_rest_base"):
        os.environ["BINANCE_BASE_URL"] = str(live.get("binance_testnet_rest_base")).rstrip("/")
    elif binance.get("binance_rest_base"):
        os.environ["BINANCE_BASE_URL"] = str(binance.get("binance_rest_base")).rstrip("/")

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
        "binance_base_url": os.environ.get("BINANCE_BASE_URL"),
    }

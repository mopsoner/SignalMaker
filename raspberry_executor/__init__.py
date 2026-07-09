"""Raspberry executor package.

The Raspberry process still boots from a local .env for connection basics, but
runtime switches are mirrored from the existing SignalMaker Admin fields when
reachable. This keeps Admin as the source of truth for live/dry-run, quote
assets, sizing, shorts, and Kraken base URL without adding duplicate settings.
"""

from __future__ import annotations

import os


def _load_local_env_once() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except Exception:
        pass


def _bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _admin_settings_url(base_url: str) -> str:
    base = str(base_url or "").rstrip("/")
    if not base:
        return ""
    if base.endswith("/api/v1"):
        return f"{base}/admin/settings"
    return f"{base}/api/v1/admin/settings"


def _bootstrap_runtime_from_admin() -> None:
    _load_local_env_once()
    base_url = os.getenv("SIGNALMAKER_BASE_URL", "")
    url = _admin_settings_url(base_url)
    if not url:
        return
    try:
        import requests

        response = requests.get(url, timeout=5)
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return
    if not isinstance(payload, dict):
        return

    live = payload.get("live") or {}
    executor = payload.get("executor") or {}
    kraken = payload.get("kraken") or {}

    if "live_trading_enabled" in live:
        dry_run_value = "false" if _bool(live.get("live_trading_enabled"), default=False) else "true"
        os.environ["DRY_RUN"] = dry_run_value
        os.environ["MARGIN_DRY_RUN"] = dry_run_value

    if "live_spot_allow_shorts" in live:
        os.environ["SHORTS_ENABLED"] = "true" if _bool(live.get("live_spot_allow_shorts"), default=False) else "false"

    if live.get("live_max_notional_per_trade") not in (None, ""):
        os.environ["ORDER_QUOTE_AMOUNT"] = str(live.get("live_max_notional_per_trade"))

    quote_assets = executor.get("quote_assets") or executor.get("QUOTE_ASSETS") or kraken.get("kraken_quote_assets")
    if quote_assets:
        os.environ["QUOTE_ASSETS"] = str(quote_assets)

    execution_exchange = executor.get("execution_exchange") or executor.get("EXECUTION_EXCHANGE") or kraken.get("execution_exchange") or kraken.get("EXECUTION_EXCHANGE")
    if execution_exchange:
        os.environ["EXECUTION_EXCHANGE"] = str(execution_exchange).strip().lower()
    if kraken.get("kraken_base_url") or kraken.get("KRAKEN_BASE_URL"):
        os.environ["KRAKEN_BASE_URL"] = str(kraken.get("kraken_base_url") or kraken.get("KRAKEN_BASE_URL")).rstrip("/")
    if kraken.get("kraken_api_key") or kraken.get("KRAKEN_API_KEY"):
        os.environ["KRAKEN_API_KEY"] = str(kraken.get("kraken_api_key") or kraken.get("KRAKEN_API_KEY"))
    if kraken.get("kraken_secret_key") or kraken.get("KRAKEN_SECRET_KEY"):
        os.environ["KRAKEN_SECRET_KEY"] = str(kraken.get("kraken_secret_key") or kraken.get("KRAKEN_SECRET_KEY"))

    if _bool(live.get("kraken_use_testnet"), default=False) and live.get("kraken_testnet_rest_base"):
        os.environ["KRAKEN_BASE_URL"] = str(live.get("kraken_testnet_rest_base")).rstrip("/")
    elif kraken.get("kraken_rest_base"):
        os.environ["KRAKEN_BASE_URL"] = str(kraken.get("kraken_rest_base")).rstrip("/")

    os.environ.setdefault("EXECUTION_MODE", "margin")
    os.environ.setdefault("MARGIN_MODE_ENABLED", "true")
    os.environ.setdefault("MARGIN_ACCOUNT_MODE", "cross")
    os.environ.setdefault("MARGIN_ISOLATED", "false")


_bootstrap_runtime_from_admin()

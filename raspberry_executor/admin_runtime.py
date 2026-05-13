from __future__ import annotations

from dataclasses import replace
from typing import Any

from raspberry_executor.margin_settings import write_margin_settings


def _bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _csv(value: Any) -> list[str]:
    return [item.strip().upper() for item in str(value or "").split(",") if item.strip()]


def _float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def apply_admin_runtime_settings(signalmaker, settings, logger=None):
    """Overlay Raspberry executor settings from the SignalMaker admin panel.

    The Raspberry still needs .env for bootstrap values such as SIGNALMAKER_BASE_URL
    and API credentials. Runtime controls come from /admin/settings when available:
      - live.live_trading_enabled -> DRY_RUN / MARGIN_DRY_RUN inverse
      - live.live_max_notional_per_trade -> ORDER_QUOTE_AMOUNT
      - live.live_spot_allow_shorts -> SHORTS_ENABLED
      - binance.binance_quote_assets -> QUOTE_ASSETS
      - binance.binance_rest_base / live testnet fields -> Binance REST base URL
    """
    try:
        admin = signalmaker.get_admin_settings()
    except Exception as exc:
        if logger:
            logger.warning("admin runtime settings unavailable, using local env settings: %s", exc)
        return settings

    live = admin.get("live") or {}
    binance = admin.get("binance") or {}

    live_enabled = _bool(live.get("live_trading_enabled"), default=not bool(settings.dry_run))
    dry_run = not live_enabled

    quote_assets = _csv(binance.get("binance_quote_assets")) or list(settings.quote_assets)
    order_quote_amount = _float(live.get("live_max_notional_per_trade"), settings.order_quote_amount)
    allow_shorts = _bool(live.get("live_spot_allow_shorts"), default=False)

    base_url = settings.binance_base_url
    use_testnet = _bool(live.get("binance_use_testnet"), default=False)
    if use_testnet and live.get("binance_testnet_rest_base"):
        base_url = str(live.get("binance_testnet_rest_base")).rstrip("/")
    elif binance.get("binance_rest_base"):
        base_url = str(binance.get("binance_rest_base")).rstrip("/")

    # Keep legacy .env-backed helpers in sync because existing modules read these
    # via margin_settings.py during the same process.
    write_margin_settings({
        "EXECUTION_MODE": "cross",
        "MARGIN_MODE_ENABLED": "true",
        "MARGIN_ACCOUNT_MODE": "cross",
        "MARGIN_ISOLATED": "false",
        "MARGIN_DRY_RUN": "true" if dry_run else "false",
        "SHORTS_ENABLED": "true" if allow_shorts else "false",
    })

    if logger:
        logger.info(
            "admin runtime settings applied dry_run=%s quote_assets=%s order_quote_amount=%s shorts_enabled=%s binance_base_url=%s",
            dry_run,
            quote_assets,
            order_quote_amount,
            allow_shorts,
            base_url,
        )

    return replace(
        settings,
        dry_run=dry_run,
        quote_assets=quote_assets,
        allowed_symbols=quote_assets,
        order_quote_amount=order_quote_amount,
        binance_base_url=base_url,
    )

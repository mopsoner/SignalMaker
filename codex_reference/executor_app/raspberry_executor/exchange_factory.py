from __future__ import annotations

from typing import Any

from raspberry_executor.config import Settings
from raspberry_executor.kraken_client import KrakenClient
from raspberry_executor.kraken_margin_client import KrakenMarginClient
from raspberry_executor.kraken_symbol_rules import KrakenSymbolRules


def exchange_name(settings: Settings) -> str:
    return str(getattr(settings, "exchange", "kraken") or "kraken").strip().lower()


def create_spot_exchange(settings: Settings) -> tuple[Any, Any]:
    """Create the configured spot execution client and symbol-rule adapter.

    The returned objects intentionally expose the same methods used by the
    existing Kraken executor code so callers can switch providers through
    EXECUTION_EXCHANGE without changing trading logic.
    """
    name = exchange_name(settings)
    if name in {"kraken", "kraken_pro"}:
        return (
            KrakenClient(settings.kraken_base_url, settings.kraken_api_key, settings.kraken_secret_key, dry_run=settings.dry_run),
            KrakenSymbolRules(settings.kraken_base_url, quote_assets=settings.quote_assets),
        )
    raise RuntimeError(f"unsupported_execution_exchange:{name}")


def create_margin_exchange(settings: Settings, *, dry_run: bool) -> tuple[Any, Any, Any]:
    """Create the configured margin execution client, margin adapter and rules.

    Kraken uses
    Kraken Spot margin semantics: margin only, implicit borrow on leveraged
    AddOrder, and implicit repay when the position is closed/settled.
    """
    name = exchange_name(settings)
    effective_dry_run = bool(settings.dry_run or dry_run)
    if name in {"kraken", "kraken_pro"}:
        client = KrakenClient(settings.kraken_base_url, settings.kraken_api_key, settings.kraken_secret_key, dry_run=effective_dry_run)
        rules = KrakenSymbolRules(settings.kraken_base_url, quote_assets=settings.quote_assets)
        return client, KrakenMarginClient(client, dry_run=effective_dry_run), rules
    raise RuntimeError(f"unsupported_execution_exchange:{name}")

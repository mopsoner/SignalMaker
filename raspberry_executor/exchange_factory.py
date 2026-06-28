from __future__ import annotations

from typing import Any

from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.binance_symbol_rules import BinanceSymbolRules
from raspberry_executor.config import Settings
from raspberry_executor.kraken_client import KrakenClient
from raspberry_executor.kraken_margin_client import KrakenMarginClient
from raspberry_executor.kraken_symbol_rules import KrakenSymbolRules
from raspberry_executor.margin_client import MarginClient


def exchange_name(settings: Settings) -> str:
    return str(getattr(settings, "exchange", "binance") or "binance").strip().lower()


def create_spot_exchange(settings: Settings) -> tuple[Any, Any]:
    """Create the configured spot execution client and symbol-rule adapter.

    The returned objects intentionally expose the same methods used by the
    existing Binance executor code so callers can switch providers through
    EXECUTION_EXCHANGE without changing trading logic.
    """
    name = exchange_name(settings)
    if name == "binance":
        return (
            BinanceClient(settings.binance_base_url, settings.binance_api_key, settings.binance_secret_key, dry_run=settings.dry_run),
            BinanceSymbolRules(settings.binance_base_url),
        )
    if name in {"kraken", "kraken_pro"}:
        return (
            KrakenClient(settings.kraken_base_url, settings.kraken_api_key, settings.kraken_secret_key, dry_run=settings.dry_run),
            KrakenSymbolRules(settings.kraken_base_url, quote_assets=settings.quote_assets),
        )
    raise RuntimeError(f"unsupported_execution_exchange:{name}")


def create_margin_exchange(settings: Settings, *, isolated: bool, dry_run: bool) -> tuple[Any, Any, Any]:
    """Create the configured margin execution client, margin adapter and rules.

    Binance keeps the existing explicit borrow/repay implementation. Kraken uses
    Kraken Spot margin semantics: cross-margin only, implicit borrow on leveraged
    AddOrder, and implicit repay when the position is closed/settled.
    """
    name = exchange_name(settings)
    effective_dry_run = bool(settings.dry_run or dry_run)
    if name == "binance":
        client = BinanceClient(settings.binance_base_url, settings.binance_api_key, settings.binance_secret_key, dry_run=effective_dry_run)
        rules = BinanceSymbolRules(settings.binance_base_url)
        return client, MarginClient(client, isolated=isolated, dry_run=effective_dry_run), rules
    if name in {"kraken", "kraken_pro"}:
        client = KrakenClient(settings.kraken_base_url, settings.kraken_api_key, settings.kraken_secret_key, dry_run=effective_dry_run)
        rules = KrakenSymbolRules(settings.kraken_base_url, quote_assets=settings.quote_assets)
        return client, KrakenMarginClient(client, isolated=isolated, dry_run=effective_dry_run), rules
    raise RuntimeError(f"unsupported_execution_exchange:{name}")

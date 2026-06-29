from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dataclasses import dataclass, field
from typing import Any, Callable

import requests

from raspberry_executor.candle_auto_feed import discover_kraken_margin_symbols, discover_kraken_spot_symbols
from raspberry_executor.candle_push_once import fetch_kraken_ohlc
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ensure_env
from raspberry_executor.kraken_client import KrakenClient
from raspberry_executor.kraken_margin_client import KrakenMarginClient
from raspberry_executor.kraken_symbol_rules import KrakenSymbolRules


DEFAULT_SYMBOL = "BTCUSD"


@dataclass
class SmokeResult:
    base_url: str
    symbol: str
    quote_assets: list[str]
    credentials_loaded: bool
    checks: list[dict[str, Any]] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)

    def add(self, name: str, ok: bool, **details: Any) -> None:
        self.checks.append({"name": name, "ok": ok, **details})

    @property
    def ok(self) -> bool:
        required_checks = [check for check in self.checks if not check.get("skipped")]
        return all(bool(check.get("ok")) for check in required_checks)

    def as_dict(self) -> dict[str, Any]:
        return {
            "exchange": "kraken",
            "base_url": self.base_url,
            "symbol": self.symbol,
            "quote_assets": self.quote_assets,
            "credentials_loaded": self.credentials_loaded,
            "ok": self.ok,
            "duration_seconds": round(time.time() - self.started_at, 3),
            "checks": self.checks,
        }


def _error_details(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return {"status_code": exc.response.status_code, "error": exc.response.text[:500]}
    return {"error": str(exc)}


def _run_check(result: SmokeResult, name: str, func: Callable[[], dict[str, Any]]) -> None:
    try:
        details = func()
        result.add(name, True, **details)
    except Exception as exc:
        result.add(name, False, **_error_details(exc))


def _find_symbol_for_quotes(base_url: str, quote_assets: list[str]) -> str:
    for quote in quote_assets:
        if quote.upper() in {"USD", "USDT", "USDC", "EUR", "GBP"}:
            return f"BTC{quote.upper()}"
    return DEFAULT_SYMBOL


def _discover_default_symbol(base_url: str, quote_assets: list[str]) -> str:
    try:
        symbols = discover_kraken_margin_symbols(base_url, quote_assets, limit=1)
        if symbols:
            return symbols[0]
    except Exception:
        pass
    try:
        symbols = discover_kraken_spot_symbols(base_url, quote_assets, limit=1)
        if symbols:
            return symbols[0]
    except Exception:
        pass
    return _find_symbol_for_quotes(base_url, quote_assets)


def _safe_sample(values: list[str], limit: int = 10) -> list[str]:
    return values[:limit]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Teste les appels Kraken publics, les adaptateurs SignalMaker, et les appels privés sans placer d'ordre réel par défaut.",
    )
    parser.add_argument(
        "--symbol",
        nargs="?",
        const="",
        default=None,
        help="Symbole à tester, ex: BTCUSDC, BTCUSDT, ETHUSDC. Si vide ou absent: première paire découverte pour QUOTE_ASSETS.",
    )
    parser.add_argument("--base-url", help="URL Kraken. Par défaut: KRAKEN_BASE_URL ou https://api.kraken.com.")
    parser.add_argument("--json", action="store_true", help="Affiche uniquement le JSON final, pratique à coller dans un ticket.")
    parser.add_argument("--skip-private", action="store_true", help="Ignore les appels privés même si les clés Kraken sont présentes.")
    parser.add_argument(
        "--validate-order",
        action="store_true",
        help="Teste AddOrder avec validate=true sur Kraken. Aucun ordre n'est placé, mais Kraken valide la paire, le volume, le type et les permissions API.",
    )
    parser.add_argument("--order-quote", type=float, default=20.0, help="Montant notionnel utilisé pour calculer un volume de test validate=true.")
    return parser


def run_smoke(args: argparse.Namespace) -> SmokeResult:
    ensure_env()
    settings = load_settings()
    base_url = (args.base_url or settings.kraken_base_url or "https://api.kraken.com").rstrip("/")
    quote_assets = settings.quote_assets or ["USD"]
    requested_symbol = str(args.symbol or "").strip()
    symbol = (requested_symbol or _discover_default_symbol(base_url, quote_assets)).upper().replace("/", "")

    client = KrakenClient(base_url, settings.kraken_api_key, settings.kraken_secret_key, dry_run=True)
    rules = KrakenSymbolRules(base_url, quote_assets=quote_assets)
    margin = KrakenMarginClient(client, isolated=True, dry_run=True)
    result = SmokeResult(base_url=base_url, symbol=symbol, quote_assets=quote_assets, credentials_loaded=client.is_configured())

    _run_check(result, "public_time", lambda: {"server_time": client._public("/0/public/Time")})
    _run_check(result, "asset_pair_lookup", lambda: {"pair": client._pair_info(symbol)})
    _run_check(result, "ticker_price", lambda: {"price": client.current_price(symbol)})
    _run_check(result, "ohlc_1h", lambda: {"count": len(fetch_kraken_ohlc(base_url, symbol, "1h", 3)), "limit": 3})

    def symbol_rules() -> dict[str, Any]:
        price = client.current_price(symbol)
        qty = rules.quantity_from_quote(symbol, args.order_quote, price, market=True)
        exit_price = rules.normalize_exit_price(symbol, price * 1.01)
        return {
            "base_asset": rules.base_asset(symbol),
            "entry_quantity_for_quote": qty,
            "normalized_exit_quantity": rules.normalize_exit_quantity(symbol, qty),
            "normalized_exit_price": exit_price,
            "oco_allowed": rules.oco_allowed(symbol),
        }

    _run_check(result, "symbol_rules", symbol_rules)
    _run_check(result, "discover_spot_symbols", lambda: {"count": len(discover_kraken_spot_symbols(base_url, quote_assets, limit=25)), "sample": _safe_sample(discover_kraken_spot_symbols(base_url, quote_assets, limit=25))})
    _run_check(result, "discover_margin_symbols", lambda: {"count": len(discover_kraken_margin_symbols(base_url, quote_assets, limit=25)), "sample": _safe_sample(discover_kraken_margin_symbols(base_url, quote_assets, limit=25))})

    def dry_run_orders() -> dict[str, Any]:
        price = client.current_price(symbol)
        qty = rules.quantity_from_quote(symbol, args.order_quote, price, market=True)
        entry = client.place_market_entry(symbol, "long", qty)
        tp = client.place_exit_limit(symbol, "long", qty, rules.normalize_exit_price(symbol, price * 1.02))
        sl = client.place_stop_loss(symbol, "long", qty, rules.normalize_exit_price(symbol, price * 0.98))
        queried = client.get_order(symbol, entry["orderId"])
        return {"quantity": qty, "entry": entry, "take_profit": tp, "stop_loss": sl, "queried_order": queried, "open_orders": client.open_orders(symbol)}

    _run_check(result, "spot_order_methods_dry_run", dry_run_orders)

    def margin_methods() -> dict[str, Any]:
        price = client.current_price(symbol)
        qty = rules.quantity_from_quote(symbol, args.order_quote, price, market=True)
        target = rules.normalize_exit_price(symbol, price * 1.02)
        return {
            "ensure_account": margin.ensure_isolated_account(symbol),
            "account": margin.isolated_account(symbol),
            "borrow": margin.borrow(symbol, rules.symbol_info(symbol).get("quoteAsset", "USD"), str(args.order_quote)),
            "repay": margin.repay(symbol, rules.symbol_info(symbol).get("quoteAsset", "USD"), str(args.order_quote)),
            "transfer": margin.transfer_spot_to_margin(symbol, rules.symbol_info(symbol).get("quoteAsset", "USD"), str(args.order_quote)),
            "entry": margin.margin_order(symbol, "BUY", qty, "MARKET"),
            "take_profit": margin.margin_order(symbol, "SELL", qty, "LIMIT", price=target),
            "open_orders": margin.open_margin_orders(symbol),
        }

    _run_check(result, "margin_methods_dry_run", margin_methods)

    if args.skip_private:
        result.add("private_account", False, skipped=True, reason="skip_private_requested")
        result.add("private_open_orders", False, skipped=True, reason="skip_private_requested")
    elif not client.is_configured():
        result.add("private_account", False, skipped=True, reason="missing_kraken_api_credentials")
        result.add("private_open_orders", False, skipped=True, reason="missing_kraken_api_credentials")
    else:
        _run_check(result, "private_account", lambda: {"balance_assets": sorted((client.account() or {}).keys())[:25]})
        live_read_client = KrakenClient(base_url, settings.kraken_api_key, settings.kraken_secret_key, dry_run=False)
        _run_check(result, "private_open_orders", lambda: {"open_orders": live_read_client.open_orders(symbol)})

        if args.validate_order:
            def validate_order() -> dict[str, Any]:
                price = client.current_price(symbol)
                qty = rules.quantity_from_quote(symbol, args.order_quote, price, market=True)
                live_client = KrakenClient(base_url, settings.kraken_api_key, settings.kraken_secret_key, dry_run=False)
                payload = {"pair": live_client._pair_key(symbol), "type": "buy", "ordertype": "market", "volume": qty, "validate": True}
                return {"quantity": qty, "response": live_client._signed("POST", "/0/private/AddOrder", payload)}

            _run_check(result, "private_add_order_validate_only", validate_order)
        else:
            result.add("private_add_order_validate_only", False, skipped=True, reason="use --validate-order to enable Kraken validate=true check")

    return result


def print_human(result: SmokeResult) -> None:
    print("\n=== Kraken full smoke test ===")
    print(f"Base URL: {result.base_url}")
    print(f"Symbol: {result.symbol}")
    print(f"Credentials loaded: {result.credentials_loaded}")
    print(f"Overall: {'PASS' if result.ok else 'FAIL'}\n")
    for check in result.checks:
        icon = "⏭️" if check.get("skipped") else ("✅" if check.get("ok") else "❌")
        print(f"{icon} {check['name']}")
        print(json.dumps({k: v for k, v in check.items() if k != "name"}, indent=2, default=str))
    print("\n=== JSON à coller pour debug ===")
    print(json.dumps(result.as_dict(), indent=2, default=str))


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_smoke(args)
    if args.json:
        print(json.dumps(result.as_dict(), indent=2, default=str))
    else:
        print_human(result)
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())

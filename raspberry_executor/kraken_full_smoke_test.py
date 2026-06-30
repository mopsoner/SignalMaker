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
from raspberry_executor.admin_settings_bridge import apply_admin_settings_to_environ
from raspberry_executor.config import Settings, load_settings
from raspberry_executor.env_store import ensure_env
from raspberry_executor.kraken_client import KrakenClient
from raspberry_executor.kraken_margin_client import KrakenMarginClient
from raspberry_executor.kraken_symbol_rules import KrakenSymbolRules
from raspberry_executor.momentum_decision_feed import build_decision_from_candidates
from raspberry_executor.signalmaker_client import SignalMakerClient


def _runtime_settings_payload() -> dict[str, Any]:
    try:
        from app.services.runtime_settings import load_runtime_settings

        return load_runtime_settings()
    except Exception as exc:
        return {"_error": str(exc)}


def _value_status(value: Any) -> dict[str, Any]:
    loaded = value not in (None, "")
    return {"loaded": loaded, "length": len(str(value)) if loaded else 0}


DEFAULT_SYMBOL = "BTCUSD"


@dataclass
class SmokeResult:
    base_url: str
    symbol: str
    quote_assets: list[str]
    credentials_loaded: bool
    credential_sources: dict[str, Any] = field(default_factory=dict)
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
            "credential_sources": self.credential_sources,
            "ok": self.ok,
            "duration_seconds": round(time.time() - self.started_at, 3),
            "checks": self.checks,
        }



def _admin_kraken_test_url(base_url: str) -> str:
    base = str(base_url or "").rstrip("/")
    if not base:
        return ""
    if base.endswith("/api/v1"):
        return f"{base}/admin/test/kraken"
    return f"{base}/api/v1/admin/test/kraken"


def _fetch_admin_kraken_credential_status(base_url: str, timeout: float = 10.0) -> dict[str, Any]:
    """Ask SignalMaker Admin whether Kraken credentials are loaded server-side.

    The Admin settings endpoint may intentionally avoid returning secret values,
    so the smoke test cannot infer server-side credentials from that payload
    alone.  The existing Kraken Admin test endpoint already checks credentials
    without exposing them; querying it lets the smoke report distinguish
    "credentials absent from this smoke process" from "credentials exist only
    inside SignalMaker Admin".
    """
    url = _admin_kraken_test_url(base_url)
    if not url:
        return {"checked": False, "reason": "missing_signalmaker_base_url"}
    try:
        response = requests.post(url, timeout=timeout)
        if getattr(response, "status_code", None) == 405:
            response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        return {"checked": False, "reason": "admin_kraken_test_unavailable", "error": str(exc)}
    if not isinstance(payload, dict):
        return {"checked": False, "reason": "invalid_admin_kraken_test_payload"}
    return {
        "checked": True,
        "status": payload.get("status"),
        "base_url": payload.get("base_url"),
        "api_key_loaded": bool(payload.get("api_key_loaded")),
        "secret_key_loaded": bool(payload.get("secret_key_loaded")),
        "error": payload.get("error"),
        "http_status": payload.get("http_status"),
    }


def _settings_with_runtime_overrides(settings: Settings, runtime: dict[str, Any] | None = None) -> Settings:
    """Return settings with DB runtime values first, then env overrides when present.

    The full Kraken smoke test is also used from the Raspberry UI/debug flow,
    where Kraken keys may live in SignalMaker Admin settings instead of the
    local .env.  This helper applies non-empty DB runtime values first, then
    non-empty environment values, to the immutable Settings object used by the
    smoke test.
    """
    import os

    overrides: dict[str, Any] = {}
    runtime = runtime or {}
    kraken_runtime = runtime.get("kraken", {}) if isinstance(runtime.get("kraken"), dict) else {}
    executor_runtime = runtime.get("executor", {}) if isinstance(runtime.get("executor"), dict) else {}
    if kraken_runtime.get("kraken_base_url"):
        overrides["kraken_base_url"] = str(kraken_runtime["kraken_base_url"]).rstrip("/")
    if kraken_runtime.get("kraken_api_key"):
        overrides["kraken_api_key"] = str(kraken_runtime["kraken_api_key"])
    if kraken_runtime.get("kraken_secret_key"):
        overrides["kraken_secret_key"] = str(kraken_runtime["kraken_secret_key"])
    if executor_runtime.get("execution_exchange"):
        overrides["exchange"] = str(executor_runtime["execution_exchange"]).strip().lower()
    if executor_runtime.get("quote_assets"):
        value = executor_runtime["quote_assets"]
        if isinstance(value, list):
            overrides["quote_assets"] = [str(item).strip().upper() for item in value if str(item).strip()]
        else:
            overrides["quote_assets"] = [item.strip().upper() for item in str(value).split(",") if item.strip()]

    mapping = {
        "KRAKEN_BASE_URL": "kraken_base_url",
        "KRAKEN_API_KEY": "kraken_api_key",
        "KRAKEN_SECRET_KEY": "kraken_secret_key",
        "EXECUTION_EXCHANGE": "exchange",
        "QUOTE_ASSETS": "quote_assets",
        "ORDER_QUOTE_AMOUNT": "order_quote_amount",
    }
    for env_key, attr in mapping.items():
        value = os.environ.get(env_key)
        if value in (None, "") or attr in overrides:
            continue
        if attr == "quote_assets":
            overrides[attr] = [item.strip().upper() for item in value.split(",") if item.strip()]
        elif attr == "order_quote_amount":
            try:
                overrides[attr] = float(value)
            except ValueError:
                continue
        elif attr == "kraken_base_url":
            overrides[attr] = value.rstrip("/")
        elif attr == "exchange":
            overrides[attr] = value.strip().lower()
        else:
            overrides[attr] = value
    if not overrides:
        return settings
    return Settings(**{**settings.__dict__, **overrides})


def _credential_sources(settings: Settings, admin_bridge: dict[str, Any], admin_kraken_test: dict[str, Any], runtime: dict[str, Any] | None = None) -> dict[str, Any]:
    import os

    settings_file = load_settings()
    kraken_runtime = (runtime or {}).get("kraken", {}) if isinstance((runtime or {}).get("kraken"), dict) else {}
    db_api = kraken_runtime.get("kraken_api_key")
    db_secret = kraken_runtime.get("kraken_secret_key")
    env_api = os.environ.get("KRAKEN_API_KEY")
    env_secret = os.environ.get("KRAKEN_SECRET_KEY")
    file_api = settings_file.kraken_api_key
    file_secret = settings_file.kraken_secret_key
    if db_api and db_secret:
        selected = "database canonical lowercase"
    elif env_api and env_secret:
        selected = "environment"
    elif file_api and file_secret:
        selected = "settings file"
    else:
        selected = "none"
    return {
        "api_key_loaded": bool(settings.kraken_api_key),
        "secret_key_loaded": bool(settings.kraken_secret_key),
        "db_kraken_api_key_loaded": _value_status(db_api),
        "db_kraken_secret_key_loaded": _value_status(db_secret),
        "env_kraken_api_key_loaded": _value_status(env_api),
        "env_kraken_secret_key_loaded": _value_status(env_secret),
        "settings_file_api_key_loaded": _value_status(file_api),
        "settings_file_secret_key_loaded": _value_status(file_secret),
        "runtime_env_api_key_loaded": bool(env_api),
        "runtime_env_secret_key_loaded": bool(env_secret),
        "selected_source": selected,
        "admin_settings_bridge": {k: v for k, v in admin_bridge.items() if k not in {"error"}},
        "admin_settings_error": admin_bridge.get("error"),
        "admin_kraken_test": admin_kraken_test,
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


def _find_candle_summary(rows: list[dict[str, Any]], symbol: str, interval: str) -> dict[str, Any] | None:
    wanted = symbol.upper()
    for row in rows:
        if str(row.get("symbol") or "").upper() == wanted and row.get("interval") == interval:
            return row
    return None

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
    parser.add_argument("--skip-signalmaker", action="store_true", help="Ignore les appels SignalMaker: candles, momentum et trade candidates.")
    parser.add_argument("--candle-intervals", default="15m,1h,4h", help="Intervalles de candles à récupérer chez Kraken puis envoyer à SignalMaker.")
    parser.add_argument("--candle-limit", type=int, default=120, help="Nombre de candles Kraken à récupérer par intervalle pour l'ingestion SignalMaker.")
    parser.add_argument("--momentum-limit", type=int, default=25, help="Nombre de lignes momentum/candidates à vérifier côté SignalMaker.")
    return parser


def run_smoke(args: argparse.Namespace) -> SmokeResult:
    ensure_env()
    args.candle_intervals = getattr(args, "candle_intervals", "15m,1h,4h")
    args.candle_limit = getattr(args, "candle_limit", 3)
    args.momentum_limit = getattr(args, "momentum_limit", 25)
    args.skip_private = getattr(args, "skip_private", False)
    args.validate_order = getattr(args, "validate_order", False)
    file_settings = load_settings()
    runtime = _runtime_settings_payload()
    admin_bridge = apply_admin_settings_to_environ(file_settings.signalmaker_base_url)
    admin_kraken_test = _fetch_admin_kraken_credential_status(file_settings.signalmaker_base_url)
    settings = _settings_with_runtime_overrides(file_settings, runtime)
    base_url = (args.base_url or settings.kraken_base_url or "https://api.kraken.com").rstrip("/")
    quote_assets = settings.quote_assets or ["USD"]
    requested_symbol = str(args.symbol or "").strip()
    symbol = (requested_symbol or _discover_default_symbol(base_url, quote_assets)).upper().replace("/", "")

    client = KrakenClient(base_url, settings.kraken_api_key, settings.kraken_secret_key, dry_run=True)
    rules = KrakenSymbolRules(base_url, quote_assets=quote_assets)
    margin = KrakenMarginClient(client, isolated=True, dry_run=True)
    result = SmokeResult(
        base_url=base_url,
        symbol=symbol,
        quote_assets=quote_assets,
        credentials_loaded=client.is_configured(),
        credential_sources=_credential_sources(settings, admin_bridge, admin_kraken_test, runtime),
    )

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

    signalmaker = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    if getattr(args, "skip_signalmaker", False):
        result.add("signalmaker_candle_feed", False, skipped=True, reason="skip_signalmaker_requested")
        result.add("signalmaker_trade_candidates", False, skipped=True, reason="skip_signalmaker_requested")
        result.add("signalmaker_market_data_momentum_ranking", False, skipped=True, reason="skip_signalmaker_requested")
    else:
        intervals = [item.strip() for item in str(args.candle_intervals).split(",") if item.strip()]

        def signalmaker_candle_feed() -> dict[str, Any]:
            endpoint = signalmaker.check_candle_ingest_endpoint()
            if not endpoint.get("ok"):
                raise RuntimeError(f"candle_ingest_endpoint_unavailable:{endpoint}")
            pushed: list[dict[str, Any]] = []
            for interval in intervals:
                before_summary = _find_candle_summary(signalmaker.candle_summary(symbol), symbol, interval)
                before_count = int(before_summary.get("candle_count", 0)) if before_summary else 0
                candles = fetch_kraken_ohlc(base_url, symbol, interval, args.candle_limit)
                if not candles:
                    raise RuntimeError(f"no_kraken_candles:{symbol}:{interval}")
                ingest = signalmaker.post_candles(symbol, interval, candles, source=f"{settings.gateway_id}-kraken-full-smoke")
                after_summary = _find_candle_summary(signalmaker.candle_summary(symbol), symbol, interval)
                latest = signalmaker.latest_candle(symbol, interval)
                after_count = int(after_summary.get("candle_count", 0)) if after_summary else 0
                if ingest.get("status") != "ok" or after_summary is None or latest is None or after_count < before_count:
                    raise RuntimeError(f"candle_ingest_not_visible:{symbol}:{interval}:ingest={ingest}:summary={after_summary}:latest={latest}")
                pushed.append({"symbol": symbol, "interval": interval, "fetched": len(candles), "upserted": ingest.get("upserted"), "before_count": before_count, "after_count": after_count, "latest_open_time": latest.get("open_time")})
            return {"endpoint": endpoint, "pushed": pushed}

        _run_check(result, "signalmaker_candle_feed", signalmaker_candle_feed)

        def signalmaker_trade_candidates() -> dict[str, Any]:
            first = signalmaker.get_recent_candidates(symbol=symbol, limit=args.momentum_limit)
            replay = signalmaker.get_recent_candidates(symbol=symbol, limit=args.momentum_limit)
            open_rows = signalmaker.get_open_candidates(limit=args.momentum_limit)
            return {"first_fetch_count": len(first), "replay_fetch_count": len(replay), "open_count": len(open_rows), "sample_ids": [row.get("candidate_id") for row in (replay or first)[:5]]}

        _run_check(result, "signalmaker_trade_candidates", signalmaker_trade_candidates)

        def signalmaker_market_data_momentum_ranking() -> dict[str, Any]:
            rankings = signalmaker.list_momentum(limit=args.momentum_limit)
            decision = build_decision_from_candidates(rankings, source="kraken_full_smoke_test")
            return {"ranking_count": len(rankings), "top_symbols": [row.get("symbol") for row in rankings[:5]], "decision_action": decision.get("action"), "decision_should_trade": decision.get("should_trade"), "decision_reason": decision.get("reason")}

        _run_check(result, "signalmaker_market_data_momentum_ranking", signalmaker_market_data_momentum_ranking)

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
        missing_reason = "missing_kraken_api_credentials"
        if admin_kraken_test.get("api_key_loaded") and admin_kraken_test.get("secret_key_loaded"):
            missing_reason = "missing_local_kraken_api_credentials_admin_has_credentials"
        result.add("private_account", False, skipped=True, reason=missing_reason)
        result.add("private_open_orders", False, skipped=True, reason=missing_reason)
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
    print("Credential diagnostics:")
    print(json.dumps(result.credential_sources, indent=2, default=str))
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

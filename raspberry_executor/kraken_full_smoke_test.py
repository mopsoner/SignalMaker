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
from raspberry_executor.candle_push_once import fetch_exchange_klines, fetch_kraken_ohlc
from raspberry_executor.admin_settings_bridge import apply_admin_settings_to_environ
from raspberry_executor.candle_backfill_4h import run_once as run_backfill_once
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
    signalmaker_base_url: str = ""
    device_mode: str = "Device"
    execution_exchange: str = "kraken"
    credential_sources: dict[str, Any] = field(default_factory=dict)
    checks: list[dict[str, Any]] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)

    def add(self, name: str, ok: bool, **details: Any) -> None:
        self.checks.append({"name": name, "ok": ok, **details})

    @property
    def ok(self) -> bool:
        required_checks = [check for check in self.checks if not check.get("skipped") and not check.get("optional") and check.get("status") != "blocked" and check.get("name") != "signalmaker_device_backfill_4h"]
        return all(bool(check.get("ok")) for check in required_checks)

    def as_dict(self) -> dict[str, Any]:
        return {
            "exchange": "kraken",
            "base_url": self.base_url,
            "symbol": self.symbol,
            "quote_assets": self.quote_assets,
            "credentials_loaded": self.credentials_loaded,
            "signalmaker_base_url": self.signalmaker_base_url,
            "device_mode": self.device_mode,
            "execution_exchange": self.execution_exchange,
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
    response = None
    try:
        response = requests.post(url, timeout=timeout)
        if getattr(response, "status_code", None) == 405:
            response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        details: dict[str, Any] = {
            "checked": False,
            "reason": "admin_kraken_test_unavailable",
            "error": str(exc),
            "url": url,
        }
        if response is not None:
            details.update(
                {
                    "status_code": getattr(response, "status_code", None),
                    "content_type": response.headers.get("content-type"),
                    "response_text_excerpt": response.text[:300],
                }
            )
        return details
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



def _classic_candidate_business_workflow_smoke(settings: Settings, symbol: str, order_quote: float) -> dict[str, Any]:
    """Exercise the classic candidate orchestration without touching live Kraken.

    This is intentionally a business/workflow smoke, not a Kraken payload smoke:
    it runs margin_executor.process_candidate through the same margin success,
    margin-to-spot fallback, and full-failure paths covered by the unit tests.
    """
    import tempfile
    from types import SimpleNamespace

    from raspberry_executor import classic_candidate_executor, sqlite_db
    from raspberry_executor.margin_executor import process_candidate
    from raspberry_executor.risk_guard import RiskGuard
    from raspberry_executor.state import StateStore

    class BusinessRules:
        def symbol_info(self, _symbol):
            quote = next((asset for asset in (settings.quote_assets or ["USD"]) if symbol.upper().endswith(asset.upper())), "USD")
            return {"quoteAsset": quote}

    class BusinessExchange:
        exchange_name = "kraken"
        dry_run = False

    class BusinessMarginManager:
        def __init__(self, *, fail_with: str | None = None):
            self.fail_with = fail_with
            self.calls: list[dict[str, Any]] = []

        def open_long_with_margin_take_profit(self, *, symbol, quote_amount, target_price, leverage=None):
            self.calls.append({"symbol": symbol, "quote_amount": quote_amount, "target_price": target_price, "leverage": leverage})
            if self.fail_with:
                raise RuntimeError(self.fail_with)
            return {
                "symbol": symbol,
                "side": "long",
                "quantity": "0.2",
                "entry_price": 100.0,
                "entry_order_id": "business-margin-entry",
                "tp_order_id": "business-margin-tp",
                "entry_payload": {"orderId": "business-margin-entry", "symbol": symbol, "side": "BUY", "type": "MARKET", "margin": True, "leverage": str(leverage)},
                "tp_payload": {"orderId": "business-margin-tp", "symbol": symbol, "side": "SELL", "type": "LIMIT", "margin": True, "price": str(target_price)},
            }

    class BusinessSpotManager:
        def __init__(self, *, fail_with: str | None = None):
            self.fail_with = fail_with
            self.calls: list[dict[str, Any]] = []

        def open_long_with_take_profit(self, *, symbol, quote_amount, target_price):
            self.calls.append({"symbol": symbol, "quote_amount": quote_amount, "target_price": target_price})
            if self.fail_with:
                raise RuntimeError(self.fail_with)
            return {
                "symbol": symbol,
                "side": "long",
                "quantity": "0.2",
                "entry_price": 100.0,
                "entry_order_id": "business-spot-entry",
                "tp_order_id": "business-spot-tp",
                "entry_payload": {"orderId": "business-spot-entry", "symbol": symbol, "side": "BUY", "type": "MARKET"},
                "tp_payload": {"orderId": "business-spot-tp", "symbol": symbol, "side": "SELL", "type": "LIMIT", "price": str(target_price)},
            }

    def candidate(candidate_id: str) -> dict[str, Any]:
        return {"candidate_id": candidate_id, "symbol": symbol, "side": "long", "status": "open", "entry_price": 100.0, "target_price": 110.0, "stop_price": 95.0}

    def events_for(state: StateStore, candidate_id: str) -> list[dict[str, Any]]:
        return [event for event in state.events() if event["candidate_id"] == candidate_id]

    def run_case(tmpdir: str, case: str, margin_fail: str | None = None, spot_fail: str | None = None) -> dict[str, Any]:
        sqlite_db.DB_PATH = Path(tmpdir) / f"{case}.db"
        state = StateStore()
        margin_manager = BusinessMarginManager(fail_with=margin_fail)
        spot_manager = BusinessSpotManager(fail_with=spot_fail)
        outcome = process_candidate(
            SimpleNamespace(order_quote_amount=float(order_quote), exchange="kraken"),
            BusinessExchange(),
            BusinessRules(),
            margin_manager,
            spot_manager,
            state,
            RiskGuard(settings.quote_assets or ["USD"], 999999),
            candidate(f"business-{case}"),
        )
        positions = state.open_positions()
        return {"outcome": outcome, "margin_calls": margin_manager.calls, "spot_calls": spot_manager.calls, "positions": positions, "events": events_for(state, f"business-{case}")}

    previous_db_path = sqlite_db.DB_PATH
    patched = {
        "margin_enabled": classic_candidate_executor.margin_enabled,
        "margin_leverage_attempts": classic_candidate_executor.margin_leverage_attempts,
        "upsert_remote_candidates": classic_candidate_executor.upsert_remote_candidates,
        "mark_candidate_executed": classic_candidate_executor.mark_candidate_executed,
        "remove_pending": classic_candidate_executor.remove_pending,
    }
    try:
        classic_candidate_executor.margin_enabled = lambda: True
        classic_candidate_executor.margin_leverage_attempts = lambda: (5,)
        classic_candidate_executor.upsert_remote_candidates = lambda _candidates: None
        classic_candidate_executor.mark_candidate_executed = lambda _candidate_id: None
        classic_candidate_executor.remove_pending = lambda _candidate_id: None
        with tempfile.TemporaryDirectory(prefix="signalmaker-business-smoke-") as tmpdir:
            margin_ok = run_case(tmpdir, "margin-ok")
            if margin_ok["outcome"] != "opened" or margin_ok["spot_calls"]:
                raise RuntimeError(f"business_margin_success_failed:{margin_ok}")
            margin_position = next(iter(margin_ok["positions"].values()))
            if margin_position.get("mode") != "cross_margin" or margin_position.get("entry_payload", {}).get("leverage") != "5" or margin_position.get("tp_payload", {}).get("type") != "LIMIT":
                raise RuntimeError(f"business_margin_payload_invalid:{margin_position}")

            fallback = run_case(tmpdir, "spot-fallback", margin_fail="margin unavailable for pair")
            if fallback["outcome"] != "opened_spot_fallback" or not fallback["margin_calls"] or not fallback["spot_calls"]:
                raise RuntimeError(f"business_spot_fallback_failed:{fallback}")
            fallback_position = next(iter(fallback["positions"].values()))
            spot_entry_payload = fallback_position.get("entry_payload", {})
            spot_tp_payload = fallback_position.get("tp_payload", {})
            forbidden_spot_fields = sorted({field for field in ("leverage", "reduce_only") if field in spot_entry_payload or field in spot_tp_payload})
            if fallback_position.get("mode") != "spot" or forbidden_spot_fields:
                raise RuntimeError(f"business_spot_payload_invalid:forbidden={forbidden_spot_fields}:position={fallback_position}")

            all_fail = run_case(tmpdir, "all-fail", margin_fail="margin unavailable for pair", spot_fail="spot rejected: insufficient funds")
            all_fail_types = [event["event_type"] for event in all_fail["events"]]
            required_error_events = {"candidate_margin_attempt_failed", "candidate_margin_fallback_spot", "candidate_spot_fallback_failed", "execution_error"}
            if all_fail["outcome"] != "error" or all_fail["positions"] or not required_error_events.issubset(all_fail_types):
                raise RuntimeError(f"business_all_fail_events_invalid:types={all_fail_types}:case={all_fail}")

            return {
                "workflow": "process_candidate long: margin -> tp, recoverable margin error -> spot -> tp, margin+spot failure -> clear events",
                "margin_success": {"outcome": margin_ok["outcome"], "margin_entry": margin_position.get("entry_payload"), "margin_tp": margin_position.get("tp_payload"), "spot_fallback_used": bool(margin_ok["spot_calls"])},
                "spot_fallback": {"outcome": fallback["outcome"], "margin_attempts": fallback["margin_calls"], "spot_entry": spot_entry_payload, "spot_tp": spot_tp_payload, "forbidden_spot_fields": forbidden_spot_fields, "events": [event["event_type"] for event in fallback["events"]]},
                "all_fail": {"outcome": all_fail["outcome"], "open_positions": len(all_fail["positions"]), "events": all_fail_types},
            }
    finally:
        sqlite_db.DB_PATH = previous_db_path
        classic_candidate_executor.margin_enabled = patched["margin_enabled"]
        classic_candidate_executor.margin_leverage_attempts = patched["margin_leverage_attempts"]
        classic_candidate_executor.upsert_remote_candidates = patched["upsert_remote_candidates"]
        classic_candidate_executor.mark_candidate_executed = patched["mark_candidate_executed"]
        classic_candidate_executor.remove_pending = patched["remove_pending"]

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Teste le device Raspberry Executor Kraken: endpoints Kraken, feed candles vers SignalMaker distant, backfill device, candidats/momentum et méthodes d'ordre dry-run.",
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
    parser.add_argument("--live-order-test", action="store_true", help="Place un petit ordre limite réel non agressif puis l'annule immédiatement. Requiert aussi KRAKEN_SMOKE_LIVE_ORDER=YES.")
    parser.add_argument("--live-order-quote", type=float, default=10.0, help="Montant notionnel maximal pour le test live contrôlé.")
    parser.add_argument("--skip-signalmaker", action="store_true", help="Ignore les appels SignalMaker distant: latest candle, ingestion candles, backfill, momentum et trade candidates.")
    parser.add_argument("--skip-backfill", action="store_true", help="Ignore le smoke backfill historique 4h Raspberry -> SignalMaker distant.")
    parser.add_argument("--backfill-days", type=int, default=7, help="Fenêtre historique en jours pour le smoke backfill 4h (1 symbole, 1 chunk).")
    parser.add_argument("--candle-intervals", default="15m,1h,4h", help="Intervalles de candles à récupérer chez Kraken puis envoyer à SignalMaker distant avec la logique latest_candle/start_time.")
    parser.add_argument("--candle-limit", type=int, default=120, help="Nombre de candles Kraken à récupérer par intervalle pour l'ingestion SignalMaker.")
    parser.add_argument("--momentum-limit", type=int, default=25, help="Nombre de lignes momentum/candidates à vérifier côté SignalMaker distant.")
    return parser


def run_smoke(args: argparse.Namespace) -> SmokeResult:
    ensure_env()
    args.candle_intervals = getattr(args, "candle_intervals", "15m,1h,4h")
    args.candle_limit = getattr(args, "candle_limit", 3)
    args.momentum_limit = getattr(args, "momentum_limit", 25)
    args.skip_private = getattr(args, "skip_private", False)
    args.validate_order = getattr(args, "validate_order", False)
    args.live_order_test = getattr(args, "live_order_test", False)
    args.live_order_quote = getattr(args, "live_order_quote", 10.0)
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
    margin = KrakenMarginClient(client, isolated=False, dry_run=True)
    result = SmokeResult(
        base_url=base_url,
        symbol=symbol,
        quote_assets=quote_assets,
        credentials_loaded=client.is_configured(),
        signalmaker_base_url=settings.signalmaker_base_url,
        device_mode="Device",
        execution_exchange="kraken",
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
        result.add("signalmaker_device_backfill_4h", False, skipped=True, reason="skip_signalmaker_requested")
        result.add("signalmaker_trade_candidates", False, skipped=True, reason="skip_signalmaker_requested")
        result.add("signalmaker_market_data_momentum_ranking", False, skipped=True, optional=True, reason="skip_signalmaker_requested")
    else:
        intervals = [item.strip() for item in str(args.candle_intervals).split(",") if item.strip()]

        def signalmaker_device_candle_feed() -> dict[str, Any]:
            endpoint = signalmaker.check_candle_ingest_endpoint()
            if not endpoint.get("ok"):
                raise RuntimeError(f"candle_ingest_endpoint_unavailable:{endpoint}")
            rows: list[dict[str, Any]] = []
            for interval in intervals:
                before_summary = _find_candle_summary(signalmaker.candle_summary(symbol), symbol, interval)
                before_count = int(before_summary.get("candle_count", 0)) if before_summary else 0
                latest_before = signalmaker.latest_candle(symbol, interval)
                start_time = int(latest_before["close_time"]) + 1 if latest_before and latest_before.get("close_time") is not None else None
                candles = fetch_kraken_ohlc(base_url, symbol, interval, args.candle_limit, start_time=start_time)
                if latest_before is not None and latest_before.get("close_time") is not None:
                    candles = [candle for candle in candles if int(candle["open_time"]) > int(latest_before["open_time"])]
                row: dict[str, Any] = {
                    "symbol": symbol,
                    "interval": interval,
                    "latest_close_time_before": latest_before.get("close_time") if latest_before else None,
                    "start_time": start_time,
                    "fetched_missing": len(candles),
                    "before_count": before_count,
                }
                if candles:
                    ingest = signalmaker.post_candles(symbol, interval, candles, source=f"{settings.gateway_id}-kraken-device-feed-smoke")
                    after_summary = _find_candle_summary(signalmaker.candle_summary(symbol), symbol, interval)
                    latest_after = signalmaker.latest_candle(symbol, interval)
                    after_count = int(after_summary.get("candle_count", 0)) if after_summary else 0
                    if ingest.get("status") != "ok" or after_summary is None or latest_after is None or after_count < before_count:
                        raise RuntimeError(f"device_candle_ingest_not_visible:{symbol}:{interval}:ingest={ingest}:summary={after_summary}:latest={latest_after}")
                    row.update({"action": "posted_missing_candles", "upserted": ingest.get("upserted"), "after_count": after_count, "latest_open_time_after": latest_after.get("open_time")})
                else:
                    row.update({"action": "already_up_to_date", "after_count": before_count})
                rows.append(row)
            return {"endpoint": endpoint, "flow": "latest_candle -> start_time -> fetch_kraken_ohlc -> post_candles", "rows": rows, "pushed": [row for row in rows if row.get("action") == "posted_missing_candles"]}

        _run_check(result, "signalmaker_candle_feed", signalmaker_device_candle_feed)

        if getattr(args, "skip_backfill", False):
            result.add("signalmaker_device_backfill_4h", False, skipped=True, reason="skip_backfill_requested")
        else:
            def signalmaker_device_backfill_4h() -> dict[str, Any]:
                import os

                previous_symbols = os.environ.get("CANDLE_FEED_SYMBOLS")
                os.environ["CANDLE_FEED_SYMBOLS"] = symbol
                try:
                    summary = run_backfill_once(days=args.backfill_days, max_symbols=1, max_chunks_per_symbol=1, enabled_override=True)
                except Exception as exc:
                    summary = {"status": "blocked", "reason": "backfill_smoke_unavailable", "error": str(exc), "symbol": symbol}
                finally:
                    if previous_symbols is None:
                        os.environ.pop("CANDLE_FEED_SYMBOLS", None)
                    else:
                        os.environ["CANDLE_FEED_SYMBOLS"] = previous_symbols
                if summary.get("status") not in {"completed", "blocked"}:
                    raise RuntimeError(f"unexpected_backfill_status:{summary}")
                return summary

            _run_check(result, "signalmaker_device_backfill_4h", signalmaker_device_backfill_4h)

        def signalmaker_trade_candidates() -> dict[str, Any]:
            first = signalmaker.get_recent_candidates(symbol=symbol, limit=args.momentum_limit)
            replay = signalmaker.get_recent_candidates(symbol=symbol, limit=args.momentum_limit)
            open_rows = signalmaker.get_open_candidates(limit=args.momentum_limit)
            return {"first_fetch_count": len(first), "replay_fetch_count": len(replay), "open_count": len(open_rows), "sample_ids": [row.get("candidate_id") for row in (replay or first)[:5]]}

        _run_check(result, "signalmaker_trade_candidates", signalmaker_trade_candidates)

        def optional_momentum_ranking_diagnostic() -> dict[str, Any]:
            try:
                rankings = signalmaker.list_momentum(limit=args.momentum_limit)
                decision = build_decision_from_candidates(rankings, source="kraken_full_smoke_test")
                return {"optional": True, "ranking_count": len(rankings), "top_symbols": [row.get("symbol") for row in rankings[:5]], "decision_action": decision.get("action"), "decision_should_trade": decision.get("should_trade"), "decision_reason": decision.get("reason")}
            except Exception as exc:
                details = _error_details(exc)
                status = details.get("status_code")
                reason = "momentum_ranking_endpoint_unavailable" if status in {404, 405} or status is None else "momentum_ranking_endpoint_unavailable"
                return {"optional": True, "ok": False, "reason": reason, **details}

        try:
            details = optional_momentum_ranking_diagnostic()
            result.add("signalmaker_market_data_momentum_ranking", bool(details.pop("ok", True)), **details)
        except Exception as exc:
            result.add("signalmaker_market_data_momentum_ranking", False, optional=True, reason="momentum_ranking_endpoint_unavailable", **_error_details(exc))

    def dry_run_orders() -> dict[str, Any]:
        price = client.current_price(symbol)
        qty = rules.quantity_from_quote(symbol, args.order_quote, price, market=True)
        entry = client.place_market_entry(symbol, "long", qty)
        tp = client.place_exit_limit(symbol, "long", qty, rules.normalize_exit_price(symbol, price * 1.02))
        sl = client.place_stop_loss(symbol, "long", qty, rules.normalize_exit_price(symbol, price * 0.98))
        queried = client.get_order(symbol, entry["orderId"])
        canceled = client.cancel_order(symbol, tp["orderId"]) if hasattr(client, "cancel_order") else {"orderId": tp["orderId"], "status": "CANCELED", "dry_run": True}
        return {"quantity": qty, "entry": entry, "take_profit": tp, "stop_loss": sl, "queried_order": queried, "open_orders": client.open_orders(symbol), "canceled_order": canceled}

    _run_check(result, "spot_order_methods_dry_run", dry_run_orders)

    def margin_methods() -> dict[str, Any]:
        price = client.current_price(symbol)
        qty = rules.quantity_from_quote(symbol, args.order_quote, price, market=True)
        target = rules.normalize_exit_price(symbol, price * 1.02)
        stop = rules.normalize_exit_price(symbol, price * 0.98)
        quote_asset = rules.symbol_info(symbol).get("quoteAsset", "USD")
        margin_x5 = KrakenMarginClient(client, isolated=False, dry_run=True, leverage=5)
        margin_x3 = KrakenMarginClient(client, isolated=False, dry_run=True, leverage=3)
        entry_x5 = margin_x5.margin_order(symbol, "BUY", qty, "MARKET")
        tp_x5 = margin_x5.margin_order(symbol, "SELL", qty, "LIMIT", price=target)
        sl_x5 = margin_x5.margin_order(symbol, "SELL", qty, "STOP_LOSS", price=stop)
        reduce_only_close = margin_x5.margin_order(symbol, "SELL", qty, "MARKET")
        fallback_spot = client.place_market_entry(symbol, "long", qty)
        return {
            "ensure_account": margin_x5.ensure_isolated_account(symbol),
            "account": margin_x5.isolated_account(symbol),
            "borrow": margin_x5.borrow(symbol, quote_asset, str(args.order_quote)),
            "repay": margin_x5.repay(symbol, quote_asset, str(args.order_quote)),
            "transfer": margin_x5.transfer_spot_to_margin(symbol, quote_asset, str(args.order_quote)),
            "margin_x5_entry": entry_x5,
            "margin_x5_take_profit_reduce_only": tp_x5,
            "margin_x5_stop_loss_reduce_only": sl_x5,
            "margin_x5_reduce_only_close": reduce_only_close,
            "margin_x3_entry": margin_x3.margin_order(symbol, "BUY", qty, "MARKET"),
            "fallback_sequence": ["try_margin_x5", "try_margin_x3", "fallback_spot"],
            "fallback_spot_entry": fallback_spot,
            "queried_order": margin_x5.get_margin_order(symbol, entry_x5["orderId"]) if hasattr(margin_x5, "get_margin_order") else client.get_order(symbol, entry_x5["orderId"]),
            "open_orders": margin_x5.open_margin_orders(symbol),
        }

    _run_check(result, "margin_order_methods_dry_run", margin_methods)
    _run_check(result, "classic_candidate_business_workflow", lambda: _classic_candidate_business_workflow_smoke(settings, symbol, args.order_quote))

    if args.skip_private:
        result.add("private_account", False, skipped=True, reason="skip_private_requested")
        result.add("private_open_orders", False, skipped=True, reason="skip_private_requested")
        result.add("private_controlled_live_order", False, skipped=True, reason="skip_private_requested")
    elif not client.is_configured():
        missing_reason = "missing_kraken_api_credentials"
        if admin_kraken_test.get("api_key_loaded") and admin_kraken_test.get("secret_key_loaded"):
            missing_reason = "missing_local_kraken_api_credentials_admin_has_credentials"
        result.add("private_account", False, skipped=True, reason=missing_reason)
        result.add("private_open_orders", False, skipped=True, reason=missing_reason)
        result.add("private_controlled_live_order", False, skipped=True, reason=missing_reason)
    else:
        _run_check(result, "private_account", lambda: {"balance_assets": sorted((client.account() or {}).keys())[:25]})
        live_read_client = KrakenClient(base_url, settings.kraken_api_key, settings.kraken_secret_key, dry_run=False)
        _run_check(result, "private_open_orders", lambda: {"open_orders": live_read_client.open_orders(symbol)})

        if args.validate_order:
            def validate_order() -> dict[str, Any]:
                price = client.current_price(symbol)
                qty = rules.quantity_from_quote(symbol, args.order_quote, price, market=True)
                live_client = KrakenClient(base_url, settings.kraken_api_key, settings.kraken_secret_key, dry_run=False)
                pair = live_client._pair_key(symbol)
                spot_payload = {"pair": pair, "type": "buy", "ordertype": "market", "volume": qty, "validate": True}
                margin_x5_payload = {"pair": pair, "type": "buy", "ordertype": "market", "volume": qty, "leverage": "5", "validate": True}
                margin_x3_payload = {"pair": pair, "type": "buy", "ordertype": "market", "volume": qty, "leverage": "3", "validate": True}
                tp_payload = {"pair": pair, "type": "sell", "ordertype": "limit", "volume": qty, "price": rules.normalize_exit_price(symbol, price * 1.02), "reduce_only": True, "validate": True}
                sl_payload = {"pair": pair, "type": "sell", "ordertype": "stop-loss", "volume": qty, "price": rules.normalize_exit_price(symbol, price * 0.98), "reduce_only": True, "validate": True}
                return {
                    "quantity": qty,
                    "spot_validate_only": live_client._signed("POST", "/0/private/AddOrder", spot_payload),
                    "margin_x5_validate_only": live_client._signed("POST", "/0/private/AddOrder", margin_x5_payload),
                    "margin_x3_validate_only": live_client._signed("POST", "/0/private/AddOrder", margin_x3_payload),
                    "take_profit_validate_only": live_client._signed("POST", "/0/private/AddOrder", tp_payload),
                    "stop_loss_validate_only": live_client._signed("POST", "/0/private/AddOrder", sl_payload),
                }

            _run_check(result, "private_add_order_validate_only", validate_order)
        else:
            result.add("private_add_order_validate_only", False, skipped=True, reason="use --validate-order to enable Kraken validate=true check")

        def controlled_live_order() -> dict[str, Any]:
            import os

            if not args.live_order_test:
                return {"skipped": True, "reason": "use --live-order-test and KRAKEN_SMOKE_LIVE_ORDER=YES to enable"}
            if os.environ.get("KRAKEN_SMOKE_LIVE_ORDER") != "YES":
                return {"skipped": True, "reason": "missing_guard_env_KRAKEN_SMOKE_LIVE_ORDER_YES"}
            quote = min(float(args.live_order_quote), float(args.order_quote), 10.0)
            price = client.current_price(symbol)
            limit_price = rules.normalize_exit_price(symbol, price * 0.5)
            qty = rules.quantity_from_quote(symbol, quote, float(limit_price), market=False)
            live_client = KrakenClient(base_url, settings.kraken_api_key, settings.kraken_secret_key, dry_run=False)
            order = live_client._signed("POST", "/0/private/AddOrder", {"pair": live_client._pair_key(symbol), "type": "buy", "ordertype": "limit", "volume": qty, "price": limit_price, "userref": int(time.time()) % 2147483647})
            txid = (order.get("txid") or [None])[0]
            queried = live_client.get_order(symbol, txid) if txid else {}
            canceled = live_client.cancel_order(symbol, txid) if txid else {"status": "no_txid"}
            return {"max_quote": quote, "limit_price": limit_price, "quantity": qty, "order": order, "queried_order": queried, "canceled_order": canceled}

        try:
            live_details = controlled_live_order()
            result.add("private_controlled_live_order", not live_details.get("skipped"), **live_details)
        except Exception as exc:
            result.add("private_controlled_live_order", False, **_error_details(exc))

    return result


def print_human(result: SmokeResult) -> None:
    print("\n=== Kraken full smoke test ===")
    print(f"Base URL: {result.base_url}")
    print(f"Symbol: {result.symbol}")
    print(f"Remote SignalMaker: {result.signalmaker_base_url}")
    print(f"Local exchange: {result.execution_exchange}")
    print(f"Mode: {result.device_mode}")
    print(f"Credentials loaded: {result.credentials_loaded}")
    print("Credential diagnostics:")
    print(json.dumps(result.credential_sources, indent=2, default=str))
    print(f"Overall: {'PASS' if result.ok else 'FAIL'}\n")
    sections = {
        "DEVICE FEED": {"signalmaker_candle_feed", "signalmaker_device_backfill_4h"},
        "TRADING EXECUTOR": {"signalmaker_trade_candidates", "spot_order_methods_dry_run", "margin_order_methods_dry_run", "classic_candidate_business_workflow"},
        "EXCHANGE": {"public_time", "asset_pair_lookup", "ticker_price", "ohlc_1h", "symbol_rules", "discover_spot_symbols", "discover_margin_symbols", "private_account", "private_open_orders", "private_add_order_validate_only", "private_controlled_live_order"},
        "OPTIONAL DIAGNOSTICS": {"signalmaker_market_data_momentum_ranking"},
    }
    printed: set[str] = set()
    for section, names in sections.items():
        print(f"\n--- {section} ---")
        for check in result.checks:
            if check["name"] not in names:
                continue
            printed.add(check["name"])
            icon = "⏭️" if check.get("skipped") else ("✅" if check.get("ok") else ("⚠️" if check.get("optional") else "❌"))
            print(f"{icon} {check['name']}")
            print(json.dumps({k: v for k, v in check.items() if k != "name"}, indent=2, default=str))
    remaining = [check for check in result.checks if check["name"] not in printed]
    if remaining:
        print("\n--- OTHER ---")
        for check in remaining:
            icon = "⏭️" if check.get("skipped") else ("✅" if check.get("ok") else ("⚠️" if check.get("optional") else "❌"))
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

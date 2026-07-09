import json
import os
import tempfile
from pathlib import Path

from raspberry_executor import sqlite_db
from raspberry_executor.kraken_client import KrakenClient
from raspberry_executor.kraken_symbol_rules import KrakenSymbolRules
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ensure_env
from raspberry_executor.margin_client import MarginClient
from raspberry_executor.margin_order_manager import MarginOrderManager
from raspberry_executor.position_sync_v2 import _order, _repair
from raspberry_executor.spot_order_manager import SpotOrderManager
from raspberry_executor.state import StateStore


def ok(name: str, **extra):
    return {"name": name, "ok": True, **extra}


def fail(name: str, error, **extra):
    return {"name": name, "ok": False, "error": str(error), **extra}


def _build_margin_position(symbol: str, quantity: str, current_price: float) -> dict:
    return {
        "candidate_id": f"smoke-margin-tp-{symbol}",
        "signal_symbol": symbol,
        "execution_symbol": symbol,
        "side": "long",
        "mode": "margin", "margin_account_mode": "cross",
        "margin_isolated": False,
        "quantity": quantity,
        "entry_price": current_price,
        "stop_price": current_price * 0.98,
        "target_price": current_price * 1.02,
        "entry_order_id": "dry-margin-entry-smoke",
        "oco_order_list_id": None,
        "tp_order_id": None,
        "sl_order_id": None,
        "exit_strategy": "take_profit_only",
        "needs_tp_replay": True,
        "source": "margin_tp_replay_smoke_test",
    }


def main() -> int:
    ensure_env()
    settings = load_settings()
    quote = settings.quote_assets[0] if settings.quote_assets else "USDT"
    symbol = os.getenv("SMOKE_SYMBOL", f"BTC{quote}").upper()

    result = {
        "status": "pending",
        "test": "margin_tp_replay_smoke_test",
        "symbol": symbol,
        "mode": "margin", "margin_account_mode": "cross",
        "dry_run": True,
        "checks": [],
    }

    with tempfile.TemporaryDirectory(prefix="signalmaker-margin-tp-smoke-") as tmp:
        original_db_path = sqlite_db.DB_PATH
        sqlite_db.DB_PATH = Path(tmp) / "raspberry_executor_smoke.db"
        try:
            client = KrakenClient(settings.kraken_base_url, settings.kraken_api_key, settings.kraken_secret_key, dry_run=True)
            rules = KrakenSymbolRules(settings.kraken_base_url)
            spot_manager = SpotOrderManager(client, rules)
            margin = MarginClient(client, dry_run=True)
            margin_manager = MarginOrderManager(client, margin, rules)
            state = StateStore()

            try:
                ping = client.session.get(f"{client.base_url}/api/v3/ping", timeout=10)
                ping.raise_for_status()
                result["checks"].append(ok("public_ping", status_code=ping.status_code))
            except Exception as exc:
                result["checks"].append(fail("public_ping", exc))
                result["status"] = "failed"
                print(json.dumps(result, indent=2))
                return 1

            try:
                info = rules.symbol_info(symbol)
                result["checks"].append(ok("symbol_info", base_asset=info.get("baseAsset"), quote_asset=info.get("quoteAsset"), status=info.get("status")))
            except Exception as exc:
                result["checks"].append(fail("symbol_info", exc))
                result["status"] = "failed"
                print(json.dumps(result, indent=2))
                return 1

            try:
                current = client.current_price(symbol)
                quantity = rules.quantity_from_quote(symbol, 20, current, market=False)
                position = _build_margin_position(symbol, quantity, current)
                candidate_id = position["candidate_id"]
                state.add_open_position(candidate_id, position)
                result["checks"].append(ok("seed_margin_position_without_tp", candidate_id=candidate_id, quantity=quantity, current_price=current))
            except Exception as exc:
                result["checks"].append(fail("seed_margin_position_without_tp", exc))
                result["status"] = "failed"
                print(json.dumps(result, indent=2))
                return 1

            try:
                repair_result = _repair(candidate_id, position, symbol, spot_manager, margin_manager, state)
                updated = state.open_positions().get(candidate_id) or {}
                checks = {
                    "repair_result": repair_result,
                    "oco_order_list_id": updated.get("oco_order_list_id"),
                    "tp_order_id": updated.get("tp_order_id"),
                    "sl_order_id": updated.get("sl_order_id"),
                    "tp_replay_mode": updated.get("tp_replay_mode"),
                    "tp_payload": updated.get("tp_payload"),
                }
                passed = (
                    repair_result == "replayed"
                    and updated.get("tp_replay_mode") == "margin"
                    and updated.get("oco_order_list_id") is None
                    and str(updated.get("tp_order_id") or "").startswith("dry-margin-tp")
                    and updated.get("sl_order_id") is None
                )
                if not passed:
                    raise RuntimeError(f"margin_tp_replay_assertion_failed:{checks}")
                result["checks"].append(ok("replay_uses_margin_take_profit", **checks))
            except Exception as exc:
                result["checks"].append(fail("replay_uses_margin_take_profit", exc))
                result["status"] = "failed"
                print(json.dumps(result, indent=2))
                return 1

            try:
                updated = state.open_positions().get(candidate_id) or {}
                tp = _order(client, margin, symbol, updated.get("tp_order_id"), use_margin=True)
                if not str((tp or {}).get("orderId") or "").startswith("dry-margin-tp"):
                    raise RuntimeError({"tp": tp})
                result["checks"].append(ok("monitor_uses_margin_tp_order_lookup", tp=tp))
            except Exception as exc:
                result["checks"].append(fail("monitor_uses_margin_order_lookup", exc))
                result["status"] = "failed"
                print(json.dumps(result, indent=2))
                return 1

            result["status"] = "ok"
            print(json.dumps(result, indent=2))
            return 0
        finally:
            sqlite_db.DB_PATH = original_db_path


if __name__ == "__main__":
    raise SystemExit(main())

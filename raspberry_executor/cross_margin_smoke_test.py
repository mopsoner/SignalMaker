import json
import sys

from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.binance_symbol_rules import BinanceSymbolRules
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ensure_env
from raspberry_executor.margin_client import MarginClient
from raspberry_executor.margin_order_manager import MarginOrderManager
from raspberry_executor.margin_settings import margin_enabled, margin_multiplier, read_margin_settings


def ok(name: str, **extra):
    return {"name": name, "ok": True, **extra}


def fail(name: str, error, **extra):
    return {"name": name, "ok": False, "error": str(error), **extra}


def main() -> int:
    ensure_env()
    settings = load_settings()
    quote = settings.quote_assets[0] if settings.quote_assets else "USDT"
    symbol = f"BTC{quote}"

    client = BinanceClient(settings.binance_base_url, settings.binance_api_key, settings.binance_secret_key, dry_run=True)
    rules = BinanceSymbolRules(settings.binance_base_url)
    margin = MarginClient(client, isolated=False, dry_run=True)
    manager = MarginOrderManager(client, margin, rules)

    result = {
        "status": "pending",
        "mode": "cross_margin",
        "symbol": symbol,
        "quote_asset": quote,
        "margin_settings": read_margin_settings(),
        "effective": {
            "margin_enabled": margin_enabled(),
            "margin_account_mode_for_this_test": "cross",
            "margin_multiplier": margin_multiplier(),
            "dry_run": True,
        },
        "checks": [],
    }

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
        result["checks"].append(ok("spot_symbol_info", symbol=symbol, base_asset=info.get("baseAsset"), quote_asset=info.get("quoteAsset"), status=info.get("status")))
    except Exception as exc:
        result["checks"].append(fail("spot_symbol_info", exc, symbol=symbol))
        result["status"] = "failed"
        print(json.dumps(result, indent=2))
        return 1

    if not client.is_configured():
        result["checks"].append(fail("signed_credentials", "missing_api_credentials"))
        result["status"] = "failed"
        print(json.dumps(result, indent=2))
        return 1

    try:
        spot_account = client._signed("GET", "/api/v3/account")
        result["checks"].append(ok("signed_spot_account", can_trade=spot_account.get("canTrade"), permissions=spot_account.get("permissions", [])))
    except Exception as exc:
        result["checks"].append(fail("signed_spot_account", exc))
        result["status"] = "failed"
        print(json.dumps(result, indent=2))
        return 1

    try:
        cross_account = client._signed("GET", "/sapi/v1/margin/account", {})
        result["checks"].append(ok("cross_margin_account", borrow_enabled=cross_account.get("borrowEnabled"), trade_enabled=cross_account.get("tradeEnabled"), transfer_enabled=cross_account.get("transferEnabled"), margin_level=cross_account.get("marginLevel")))
    except Exception as exc:
        result["checks"].append(fail("cross_margin_account", exc, note="API key may not have margin permission or margin account may not be enabled."))

    try:
        max_borrow = client._signed("GET", "/sapi/v1/margin/maxBorrowable", {"asset": quote})
        result["checks"].append(ok("cross_max_borrowable_quote", asset=quote, response=max_borrow))
    except Exception as exc:
        result["checks"].append(fail("cross_max_borrowable_quote", exc, asset=quote))

    try:
        dry_transfer = margin.transfer_spot_to_margin(symbol, quote, "1")
        result["checks"].append(ok("dry_run_cross_transfer_payload", payload=dry_transfer))
    except Exception as exc:
        result["checks"].append(fail("dry_run_cross_transfer_payload", exc))

    try:
        dry_borrow = margin.borrow(symbol, quote, "1")
        result["checks"].append(ok("dry_run_cross_borrow_payload", payload=dry_borrow))
    except Exception as exc:
        result["checks"].append(fail("dry_run_cross_borrow_payload", exc))

    try:
        current = client.current_price(symbol)
        qty = rules.quantity_from_quote(symbol, 20, current, market=True)
        order = margin.margin_order(symbol, "BUY", qty)
        result["checks"].append(ok("dry_run_cross_margin_buy_payload", current_price=current, quantity=qty, payload=order))
    except Exception as exc:
        result["checks"].append(fail("dry_run_cross_margin_buy_payload", exc))

    try:
        current = client.current_price(symbol)
        qty = rules.quantity_from_quote(symbol, 20, current, market=False)
        oco = manager.create_margin_oco_sell(symbol=symbol, quantity=qty, target_price=current * 1.02, stop_price=current * 0.98)
        result["checks"].append(ok("dry_run_cross_margin_oco_payload", payload=oco))
    except Exception as exc:
        result["checks"].append(fail("dry_run_cross_margin_oco_payload", exc))

    hard_fail_names = {"public_ping", "spot_symbol_info", "signed_spot_account"}
    hard_fail = any((not item.get("ok")) and item.get("name") in hard_fail_names for item in result["checks"])
    result["status"] = "failed" if hard_fail else "ok"
    print(json.dumps(result, indent=2))
    return 1 if hard_fail else 0


if __name__ == "__main__":
    sys.exit(main())

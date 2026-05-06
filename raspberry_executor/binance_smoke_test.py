import json
import sys

import requests

from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ensure_env


def main() -> int:
    ensure_env()
    settings = load_settings()
    client = BinanceClient(
        settings.binance_base_url,
        settings.binance_api_key,
        settings.binance_secret_key,
        dry_run=True,
    )

    result = {
        "base_url": settings.binance_base_url,
        "dry_run": True,
        "api_key_loaded": bool(settings.binance_api_key),
        "secret_key_loaded": bool(settings.binance_secret_key),
        "checks": [],
    }

    try:
        ping = client.session.get(f"{client.base_url}/api/v3/ping", timeout=10)
        ping.raise_for_status()
        result["checks"].append({"name": "public_ping", "ok": True, "status_code": ping.status_code})
    except Exception as exc:
        result["checks"].append({"name": "public_ping", "ok": False, "error": str(exc)})
        print(json.dumps(result, indent=2))
        return 1

    symbol = "BTCUSDT"
    try:
        price = client.current_price(symbol)
        result["checks"].append({"name": "ticker_price", "ok": True, "symbol": symbol, "price": price})
    except Exception as exc:
        result["checks"].append({"name": "ticker_price", "ok": False, "symbol": symbol, "error": str(exc)})
        print(json.dumps(result, indent=2))
        return 1

    if not client.is_configured():
        result["checks"].append({"name": "signed_account", "ok": False, "error": "missing_api_credentials"})
        print(json.dumps(result, indent=2))
        return 1

    try:
        account = client._signed("GET", "/api/v3/account")
        balances = account.get("balances") or []
        non_zero = [
            item for item in balances
            if float(item.get("free", 0) or 0) > 0 or float(item.get("locked", 0) or 0) > 0
        ]
        result["checks"].append({
            "name": "signed_account",
            "ok": True,
            "can_trade": account.get("canTrade"),
            "account_type": account.get("accountType"),
            "permissions": account.get("permissions", []),
            "non_zero_balance_count": len(non_zero),
            "non_zero_balance_assets": [item.get("asset") for item in non_zero[:20]],
        })
    except requests.HTTPError as exc:
        response = exc.response
        detail = response.text[:500] if response is not None else str(exc)
        result["checks"].append({"name": "signed_account", "ok": False, "status_code": getattr(response, "status_code", None), "error": detail})
        print(json.dumps(result, indent=2))
        return 1
    except Exception as exc:
        result["checks"].append({"name": "signed_account", "ok": False, "error": str(exc)})
        print(json.dumps(result, indent=2))
        return 1

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

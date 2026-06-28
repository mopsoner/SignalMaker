import json
import sys

import requests

from raspberry_executor.candle_auto_feed import discover_symbols_for_exchange
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ensure_env
from raspberry_executor.exchange_factory import create_spot_exchange, exchange_name
from raspberry_executor.margin_settings import execution_mode


def _probe_symbol(settings, exchange: str) -> str:
    for quote in settings.quote_assets:
        quote = quote.upper()
        if exchange in {"kraken", "kraken_pro"}:
            if quote in {"USD", "USDT", "USDC", "EUR", "GBP"}:
                return f"BTC{quote}"
        elif quote in {"USDT", "USDC", "FDUSD", "USD"}:
            return f"BTC{quote}"
    return "BTCUSD" if exchange in {"kraken", "kraken_pro"} else "BTCUSDT"


def main() -> int:
    ensure_env()
    settings = load_settings()
    exchange = exchange_name(settings)
    client, _rules = create_spot_exchange(settings)
    base_url = settings.kraken_base_url if exchange in {"kraken", "kraken_pro"} else settings.binance_base_url
    result = {
        "exchange": exchange,
        "base_url": base_url,
        "dry_run": True,
        "api_key_loaded": bool(settings.kraken_api_key if exchange in {"kraken", "kraken_pro"} else settings.binance_api_key),
        "secret_key_loaded": bool(settings.kraken_secret_key if exchange in {"kraken", "kraken_pro"} else settings.binance_secret_key),
        "quote_assets": settings.quote_assets,
        "checks": [],
    }

    try:
        if exchange in {"kraken", "kraken_pro"}:
            response = client.session.get(f"{client.base_url}/0/public/Time", timeout=10)
        else:
            response = client.session.get(f"{client.base_url}/api/v3/ping", timeout=10)
        response.raise_for_status()
        result["checks"].append({"name": "public_ping", "ok": True, "status_code": response.status_code})
    except Exception as exc:
        result["checks"].append({"name": "public_ping", "ok": False, "error": str(exc)})
        print(json.dumps(result, indent=2))
        return 1

    symbol = _probe_symbol(settings, exchange)
    try:
        price = client.current_price(symbol)
        result["checks"].append({"name": "ticker_price", "ok": True, "symbol": symbol, "price": price})
    except Exception as exc:
        result["checks"].append({"name": "ticker_price", "ok": False, "symbol": symbol, "error": str(exc)})
        print(json.dumps(result, indent=2))
        return 1

    try:
        symbols, source = discover_symbols_for_exchange(settings, settings.quote_assets, execution_mode(), limit=0)
        result["checks"].append({"name": "symbols_for_quote", "ok": True, "source": source, "count": len(symbols), "sample": symbols[:20]})
    except Exception as exc:
        result["checks"].append({"name": "symbols_for_quote", "ok": False, "error": str(exc)})
        print(json.dumps(result, indent=2))
        return 1

    if not client.is_configured():
        result["checks"].append({"name": "signed_account", "ok": False, "skipped": True, "error": "missing_api_credentials"})
        print(json.dumps(result, indent=2))
        return 0

    try:
        account = client.account()
        result["checks"].append({"name": "signed_account", "ok": True, "account_keys": sorted(account.keys())[:20] if isinstance(account, dict) else []})
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

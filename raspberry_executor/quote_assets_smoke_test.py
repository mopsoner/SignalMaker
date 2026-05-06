import json
import sys

from raspberry_executor.candle_auto_feed import discover_spot_symbols
from raspberry_executor.config import load_settings
from raspberry_executor.env_store import ensure_env


def main() -> int:
    ensure_env()
    settings = load_settings()
    result = {
        "status": "pending",
        "base_url": settings.binance_base_url,
        "quote_assets": settings.quote_assets,
        "checks": [],
    }
    try:
        symbols = discover_spot_symbols(settings.binance_base_url, settings.quote_assets, limit=0)
        result.update({
            "status": "ok",
            "total_spot_tradeable_symbol_count_for_quote": len(symbols),
            "total_spot_tradeable_symbols_sample": symbols[:20],
        })
        result["checks"].append({
            "name": "spot_symbols_for_quote",
            "ok": True,
            "quote_assets": settings.quote_assets,
            "total_spot_tradeable_symbol_count_for_quote": len(symbols),
        })
        print(json.dumps(result, indent=2))
        return 0
    except Exception as exc:
        result["status"] = "failed"
        result["checks"].append({
            "name": "spot_symbols_for_quote",
            "ok": False,
            "quote_assets": settings.quote_assets,
            "error": str(exc),
        })
        print(json.dumps(result, indent=2))
        return 1


if __name__ == "__main__":
    sys.exit(main())

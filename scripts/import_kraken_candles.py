#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.session import SessionLocal
from app.services.kraken_candle_importer import import_kraken_candles


def csv_list(value: str) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Import recent Kraken OHLC candles directly inside main DB.")
    parser.add_argument("--quotes", default="USD", help="Comma-separated quote assets, e.g. USD or USD,USDC")
    parser.add_argument("--intervals", default="4h", help="Comma-separated intervals, e.g. 4h or 15m,1h,4h")
    parser.add_argument("--limit", type=int, default=120, help="Candles per symbol/interval request. Kraken public OHLC is capped by Kraken.")
    parser.add_argument("--max-symbols", type=int, default=0, help="Maximum symbols to import. 0 means all discovered symbols.")
    parser.add_argument("--requests-per-minute", type=int, default=60, help="Kraken public request throttle.")
    parser.add_argument("--base-url", default="https://api.kraken.com", help="Kraken base URL.")

    margin_group = parser.add_mutually_exclusive_group()
    margin_group.add_argument("--margin-only", dest="margin_only", action="store_true", default=True, help="Import only Kraken margin-eligible pairs. Default.")
    margin_group.add_argument("--include-spot", dest="margin_only", action="store_false", help="Include all spot pairs for the selected quotes.")

    args = parser.parse_args()

    db = SessionLocal()
    try:
        result = import_kraken_candles(
            db=db,
            quote_assets=csv_list(args.quotes),
            intervals=csv_list(args.intervals),
            limit=args.limit,
            max_symbols=args.max_symbols,
            margin_only=args.margin_only,
            base_url=args.base_url,
            requests_per_minute=args.requests_per_minute,
        )
        print(json.dumps(result, indent=2, default=str))
        return 0 if result.get("status") == "ok" else 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())

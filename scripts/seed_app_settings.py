#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.session import SessionLocal  # noqa: E402
from app.services.runtime_settings import seed_app_settings_from_env  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Seed canonical app_settings rows from .env and DEFAULT_SETTINGS."
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Explicit resync: overwrite existing non-empty app_settings rows with .env/default values.",
    )
    args = parser.parse_args()

    db = SessionLocal()
    try:
        result = seed_app_settings_from_env(db, overwrite_empty_only=not args.overwrite)
    finally:
        db.close()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

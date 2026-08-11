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
from app.services.runtime_settings import cleanup_legacy_app_settings  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Back up app_settings and remove obsolete legacy runtime-setting rows."
    )
    parser.add_argument(
        "--backup-path",
        default=None,
        help="Backup JSON file or directory. Defaults to backups/app_settings_before_legacy_cleanup_<timestamp>.json.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write the backup and report matching legacy rows without deleting them.",
    )
    args = parser.parse_args()

    db = SessionLocal()
    try:
        result = cleanup_legacy_app_settings(db, args.backup_path, dry_run=args.dry_run)
    finally:
        db.close()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

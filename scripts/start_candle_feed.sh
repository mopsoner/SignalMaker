#!/usr/bin/env bash
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"
source .venv/bin/activate
exec python -c 'from raspberry_executor.candle_auto_feed import run_loop; run_loop()'

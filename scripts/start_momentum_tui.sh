#!/usr/bin/env bash
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"
source .venv/bin/activate
export SIGNALMAKER_API_BASE="${SIGNALMAKER_API_BASE:-http://127.0.0.1:8080}"
export MOMENTUM_TUI_REFRESH="${MOMENTUM_TUI_REFRESH:-30}"
exec python scripts/momentum_tui.py

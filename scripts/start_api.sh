#!/usr/bin/env bash
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"

if [ ! -x ".venv/bin/python" ]; then
  if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 is required to start the SignalMaker API" >&2
    exit 1
  fi
  python3 -m venv .venv --system-site-packages
fi

PYTHON=".venv/bin/python"
if ! "$PYTHON" -c "import uvicorn" >/dev/null 2>&1; then
  REQUIREMENTS_FILE="${REQUIREMENTS_FILE:-requirements.txt}"
  if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "Requirements file not found: $REQUIREMENTS_FILE" >&2
    exit 1
  fi
  "$PYTHON" -m pip install -q -r "$REQUIREMENTS_FILE"
fi

exec "$PYTHON" -m uvicorn app.main:app --host 0.0.0.0 --port "${APP_PORT:-5000}"

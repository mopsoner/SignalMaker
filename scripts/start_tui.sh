#!/usr/bin/env bash
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"

if [ -d ".venv" ]; then
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
fi

export TERM="${TERM:-linux}"
export SIGNALMAKER_BASE_URL="${SIGNALMAKER_BASE_URL:-http://127.0.0.1:5000}"

if ! python - <<'PY' >/dev/null 2>&1
import curses
PY
then
  echo "Missing Python curses support. Install it with: sudo apt install -y python3-curses" >&2
  exit 1
fi

exec python -m raspberry_executor.tui

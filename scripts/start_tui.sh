#!/usr/bin/env bash
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"

if [ -d ".venv" ]; then
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
fi

export TERM="${TERM:-linux}"
if [ -z "${APP_PORT:-}" ] && [ -f ".env" ]; then
  APP_PORT="$(awk -F= '$1 == "APP_PORT" {print $2; exit}' .env)"
fi
APP_PORT="${APP_PORT:-8080}"
export SIGNALMAKER_BASE_URL="${SIGNALMAKER_BASE_URL:-http://127.0.0.1:${APP_PORT}}"

wait_for_api() {
  local base_url="${SIGNALMAKER_BASE_URL%/}"
  local health_url="${SIGNALMAKER_HEALTH_URL:-${base_url}/healthz}"
  local timeout_seconds="${API_STARTUP_TIMEOUT:-300}"
  local check_interval_seconds="${API_STARTUP_CHECK_INTERVAL:-30}"

  if [ "$timeout_seconds" -lt 300 ]; then
    timeout_seconds=300
  fi

  local deadline=$((SECONDS + timeout_seconds))

  echo "Waiting for SignalMaker API at ${health_url} before starting TUI..."
  echo "Health check timeout: ${timeout_seconds}s; interval: ${check_interval_seconds}s."
  while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -fsS --max-time 2 "$health_url" >/dev/null 2>&1; then
      echo "SignalMaker API is ready; starting TUI."
      return 0
    fi
    local remaining=$((deadline - SECONDS))
    if [ "$remaining" -le 0 ]; then
      break
    fi
    if [ "$remaining" -lt "$check_interval_seconds" ]; then
      sleep "$remaining"
    else
      sleep "$check_interval_seconds"
    fi
  done

  echo "SignalMaker API did not become ready at ${health_url} within ${timeout_seconds}s; TUI will not start." >&2
  return 1
}

if ! python - <<'PY' >/dev/null 2>&1
import curses
PY
then
  echo "Missing Python curses support. Install it with: sudo apt install -y python3-curses" >&2
  exit 1
fi

wait_for_api

exec python -m raspberry_executor.tui

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


HEALTH_URL="${SIGNALMAKER_HEALTH_URL:-${SIGNALMAKER_BASE_URL%/}/healthz}"
WAIT_SECONDS="${SIGNALMAKER_WAIT_FOR_API_SECONDS:-60}"
printf 'Waiting for SignalMaker API at %s' "$HEALTH_URL"
ready=0
for _ in $(seq 1 "$WAIT_SECONDS"); do
  if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
    ready=1
    break
  fi
  printf '.'
  sleep 1
done
printf '\n'
if [ "$ready" != "1" ]; then
  echo "SignalMaker API did not become ready after ${WAIT_SECONDS} seconds: $HEALTH_URL" >&2
  echo "Start the API first, for example: sudo systemctl start signalmaker-api" >&2
  exit 1
fi

exec python -m raspberry_executor.tui

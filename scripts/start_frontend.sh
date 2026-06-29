#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DIST_DIR="$APP_DIR/frontend/dist"

if [ "${RUN_STANDALONE_FRONTEND:-0}" != "1" ]; then
  echo "Standalone frontend server disabled: signalmaker-api serves frontend/dist on APP_PORT=${APP_PORT:-5000}."
  echo "Open http://127.0.0.1:${APP_PORT:-5000}/admin.html"
  echo "Set RUN_STANDALONE_FRONTEND=1 to run the legacy static server for development only."
  exit 0
fi

FRONTEND_PORT="${FRONTEND_PORT:-5001}"
if [ ! -d "$DIST_DIR" ]; then
  echo "frontend/dist is missing. Run 'bash scripts/build_frontend.sh' first; this lightweight build only copies HTML/CSS/JS and does not use Vite/esbuild." >&2
  exit 1
fi
cd "$DIST_DIR"
echo "SignalMaker optional standalone frontend serving frontend/dist on port ${FRONTEND_PORT}"
exec python3 -m http.server "$FRONTEND_PORT" --bind 0.0.0.0

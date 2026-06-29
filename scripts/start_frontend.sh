#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
FRONTEND_PORT="${FRONTEND_PORT:-3000}"
DIST_DIR="$APP_DIR/frontend/dist"

if [ ! -d "$DIST_DIR" ]; then
  echo "frontend/dist is missing. Build the frontend on a compatible machine, then copy frontend/dist to this Raspberry Pi." >&2
  exit 1
fi

cd "$DIST_DIR"
exec python3 -m http.server "$FRONTEND_PORT" --bind 0.0.0.0
FRONTEND_DIR="$APP_DIR/frontend"
DIST_DIR="$FRONTEND_DIR/dist"

if [ -d "$DIST_DIR" ]; then
  echo "SignalMaker frontend serving prebuilt dist on port ${FRONTEND_PORT}"
  echo "API base: ${VITE_API_BASE:-http://127.0.0.1:8080}"
  cd "$DIST_DIR"
  exec python3 -m http.server "${FRONTEND_PORT}" --bind 0.0.0.0
fi

if [ "${FRONTEND_DEV_SERVER:-false}" != "true" ]; then
  echo "frontend/dist is missing. Run npm run build on a compatible machine or set FRONTEND_DEV_SERVER=true to use Vite dev server." >&2
  exit 1
fi

if ! command -v npm >/dev/null 2>&1; then
  echo "npm is required to run the Vite dev server" >&2
  exit 1
fi

cd "$FRONTEND_DIR"

if [ ! -d node_modules ]; then
  npm install
fi

echo "SignalMaker frontend starting Vite dev server on port ${FRONTEND_PORT}"
echo "API base: ${VITE_API_BASE:-http://127.0.0.1:8080}"

exec npm run dev -- --host 0.0.0.0 --port "${FRONTEND_PORT}"

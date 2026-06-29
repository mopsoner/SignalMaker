#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
FRONTEND_PORT="${FRONTEND_PORT:-3000}"
DIST_DIR="$APP_DIR/frontend/dist"

if [ ! -d "$DIST_DIR" ]; then
  echo "frontend/dist is missing. Run 'bash scripts/build_frontend.sh' first; this lightweight build only copies HTML/CSS/JS and does not use Vite/esbuild." >&2
  exit 1
fi

echo "SignalMaker lightweight frontend serving frontend/dist on port ${FRONTEND_PORT}"
cd "$DIST_DIR"
exec python3 -m http.server "$FRONTEND_PORT" --bind 0.0.0.0

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

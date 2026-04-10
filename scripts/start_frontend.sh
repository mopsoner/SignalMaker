#!/usr/bin/env bash
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR/frontend"

if ! command -v npm >/dev/null 2>&1; then
  echo "npm is required to run the frontend"
  exit 1
fi

if [ ! -d node_modules ]; then
  npm install
fi

exec npm run dev -- --host 0.0.0.0 --port ${FRONTEND_PORT:-3000}

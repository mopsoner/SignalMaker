#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"

bash run.sh init-db

APP_PORT=${APP_PORT:-8080} bash run.sh api &
API_PID=$!

cleanup() {
  kill "$API_PID" 2>/dev/null || true
}

trap cleanup EXIT INT TERM

FRONTEND_PORT=${FRONTEND_PORT:-5000} bash scripts/start_frontend.sh

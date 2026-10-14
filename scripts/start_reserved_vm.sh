#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"

RUNTIME_DIR="$APP_DIR/.runtime"
export SIGNALMAKER_LOG_DIR="${SIGNALMAKER_LOG_DIR:-$APP_DIR/logs}"
export SIGNALMAKER_WORKER_SUPERVISOR=local
LOG_DIR="$SIGNALMAKER_LOG_DIR"
mkdir -p "$RUNTIME_DIR" "$LOG_DIR"

bash run.sh init-db

bash run.sh api &
API_PID=$!

# The API starts essential and flag-controlled live workers via WorkerControlService.

FRONTEND_PORT=${FRONTEND_PORT:-5000} bash scripts/start_frontend.sh &
FRONTEND_PID=$!

cleanup() {
  kill "$API_PID" "$FRONTEND_PID" 2>/dev/null || true
}

trap cleanup EXIT INT TERM

wait "$FRONTEND_PID"

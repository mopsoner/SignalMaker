#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"

bash run.sh init-db

bash run.sh api &
API_PID=$!

bash scripts/start_pipeline_worker.sh &
PIPELINE_PID=$!

bash scripts/start_executor_worker.sh &
EXECUTOR_PID=$!

bash scripts/start_scheduler_worker.sh &
SCHEDULER_PID=$!

FRONTEND_PORT=${FRONTEND_PORT:-5000} bash scripts/start_frontend.sh &
FRONTEND_PID=$!

cleanup() {
  kill "$API_PID" "$PIPELINE_PID" "$EXECUTOR_PID" "$SCHEDULER_PID" "$FRONTEND_PID" 2>/dev/null || true
}

trap cleanup EXIT INT TERM

wait "$FRONTEND_PID"

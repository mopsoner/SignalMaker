#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"

python -m scripts.init_db

bash scripts/start_api.sh &
API_PID=$!

bash scripts/start_pipeline_worker.sh &
PIPELINE_PID=$!

bash scripts/start_executor_worker.sh &
EXECUTOR_PID=$!

bash scripts/start_scheduler_worker.sh &
SCHEDULER_PID=$!

if [ "${RUN_FRONTEND:-0}" = "1" ]; then
  RUN_STANDALONE_FRONTEND=1 FRONTEND_PORT=${FRONTEND_PORT:-5001} bash scripts/start_frontend.sh &
  FRONTEND_PID=$!
else
  FRONTEND_PID=""
  echo "Lightweight static frontend disabled by default to reduce Raspberry Pi CPU usage."
  echo "Set RUN_FRONTEND=1 to start the optional development-only standalone server anyway."
fi

cleanup() {
  kill "$API_PID" "$PIPELINE_PID" "$EXECUTOR_PID" "$SCHEDULER_PID" 2>/dev/null || true
  if [ -n "$FRONTEND_PID" ]; then
    kill "$FRONTEND_PID" 2>/dev/null || true
  fi
}

trap cleanup EXIT INT TERM

wait "$API_PID"

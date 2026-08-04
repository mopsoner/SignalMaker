#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"

RUNTIME_DIR="$APP_DIR/.runtime"
mkdir -p "$RUNTIME_DIR"

WORKER_PIDS=()
WORKER_PID_FILES=()

start_worker() {
  local name="$1"
  local script="$2"
  local pid

  bash "$script" &
  pid=$!

  printf -v "${name^^}_PID" '%s' "$pid"
  printf '%s\n' "$pid" > "$RUNTIME_DIR/$name.pid"
  WORKER_PIDS+=("$pid")
  WORKER_PID_FILES+=("$RUNTIME_DIR/$name.pid")
}

bash run.sh init-db

bash run.sh api &
API_PID=$!

start_worker "ibkr_ingestion" "scripts/start_ibkr_ingestion_worker.sh"
start_worker "stock_etf_analysis" "scripts/start_market_analysis_worker.sh"
start_worker "scheduler" "scripts/start_scheduler_worker.sh"

FRONTEND_PORT=${FRONTEND_PORT:-5000} bash scripts/start_frontend.sh &
FRONTEND_PID=$!

cleanup() {
  kill "$API_PID" "${WORKER_PIDS[@]}" "$FRONTEND_PID" 2>/dev/null || true
  rm -f "${WORKER_PID_FILES[@]}"
}

trap cleanup EXIT INT TERM

wait "$FRONTEND_PID"

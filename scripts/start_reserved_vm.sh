#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"

RUNTIME_DIR="$APP_DIR/.runtime"
export SIGNALMAKER_LOG_DIR="${SIGNALMAKER_LOG_DIR:-$APP_DIR/logs}"
LOG_DIR="$SIGNALMAKER_LOG_DIR"
mkdir -p "$RUNTIME_DIR" "$LOG_DIR"

WORKER_PIDS=()
WORKER_PID_FILES=()

start_worker() {
  local name="$1"
  local script="$2"
  local pid

  mkdir -p "$LOG_DIR"
  bash "$script" >> "$LOG_DIR/$name.log" 2>&1 &
  pid=$!

  printf -v "${name^^}_PID" '%s' "$pid"
  printf '%s\n' "$pid" > "$RUNTIME_DIR/$name.pid"
  WORKER_PIDS+=("$pid")
  WORKER_PID_FILES+=("$RUNTIME_DIR/$name.pid")
}

bash run.sh init-db

bash run.sh api &
API_PID=$!

start_worker "pipeline" "scripts/start_pipeline_worker.sh"
start_worker "executor" "scripts/start_executor_worker.sh"
start_worker "kraken_candle_feed" "scripts/start_kraken_candle_feed_worker.sh"
start_worker "momentum_engine" "scripts/start_momentum_engine_worker.sh"
if [ "${MOMENTUM_EXECUTION_ENABLED:-false}" = "true" ]; then
  start_worker "momentum_executor" "scripts/start_momentum_executor_worker.sh"
fi
start_worker "momentum_backtest" "scripts/start_momentum_backtest_worker.sh"
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

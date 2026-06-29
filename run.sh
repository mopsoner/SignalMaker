#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

usage() {
  cat <<'USAGE'
Usage: ./run.sh [command]

Commands:
  api             Start the FastAPI backend on APP_PORT (default: 5000)
  init-db         Initialize database tables
  frontend        Start the lightweight static frontend
  pipeline-loop   Start the pipeline worker loop
  executor-loop   Start the executor worker loop
  scheduler-loop  Start the scheduler worker loop
  all             Start API first, then frontend after the API is reachable
  reserved-vm     Alias for all

If no command is provided, the API and frontend are started.
USAGE
}

wait_for_api() {
  local port="${APP_PORT:-5000}"
  local health_url="http://127.0.0.1:${port}/healthz"
  local timeout_seconds="${API_STARTUP_TIMEOUT:-60}"
  local deadline=$((SECONDS + timeout_seconds))

  echo "Waiting for SignalMaker backend at ${health_url} before starting the frontend..."
  while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -fsS --max-time 2 "$health_url" >/dev/null 2>&1; then
      echo "SignalMaker backend is ready; starting frontend."
      return 0
    fi
    sleep 1
  done

  echo "Backend did not become ready at ${health_url} within ${timeout_seconds}s; frontend will not start." >&2
  return 1
}

start_api_and_frontend() {
  bash scripts/start_api.sh "$@" &
  API_PID=$!
  FRONTEND_PID=""

  cleanup() {
    if [ -n "${FRONTEND_PID}" ]; then
      kill "$FRONTEND_PID" 2>/dev/null || true
    fi
    kill "$API_PID" 2>/dev/null || true
  }

  trap cleanup EXIT INT TERM

  if ! wait_for_api; then
    kill "$API_PID" 2>/dev/null || true
    wait "$API_PID" 2>/dev/null || true
    exit 1
  fi

  bash scripts/start_frontend.sh &
  FRONTEND_PID=$!

  wait -n "$API_PID" "$FRONTEND_PID"
}

command="${1:-all}"
if [ "$#" -gt 0 ]; then
  shift
fi

case "$command" in
  api)
    exec bash scripts/start_api.sh "$@"
    ;;
  init-db)
    exec python -m scripts.init_db "$@"
    ;;
  frontend)
    exec bash scripts/start_frontend.sh "$@"
    ;;
  pipeline-loop)
    exec bash scripts/start_pipeline_worker.sh "$@"
    ;;
  executor-loop)
    exec bash scripts/start_executor_worker.sh "$@"
    ;;
  scheduler-loop)
    exec bash scripts/start_scheduler_worker.sh "$@"
    ;;
  all|reserved-vm)
    start_api_and_frontend "$@"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "Unknown command: $command" >&2
    usage >&2
    exit 2
    ;;
esac

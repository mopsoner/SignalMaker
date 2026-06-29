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
  all             Start API first, then workers/executor and frontend after the API is reachable
  reserved-vm     Alias for all

If no command is provided, the API, workers/executor, and frontend are started.
USAGE
}

wait_for_api() {
  local port="${APP_PORT:-5000}"
  local health_url="http://127.0.0.1:${port}/healthz"
  local timeout_seconds="${API_STARTUP_TIMEOUT:-60}"
  local deadline=$((SECONDS + timeout_seconds))

  echo "Waiting for SignalMaker backend at ${health_url} before starting workers/executor and frontend..."
  while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -fsS --max-time 2 "$health_url" >/dev/null 2>&1; then
      echo "SignalMaker backend is ready; starting workers/executor and frontend."
      return 0
    fi
    sleep 1
  done

  echo "Backend did not become ready at ${health_url} within ${timeout_seconds}s; workers/executor and frontend will not start." >&2
  return 1
}

start_api_workers_and_frontend() {
  local pids=()
  local started_api=false

  cleanup() {
    if [ "${#pids[@]}" -gt 0 ]; then
      kill "${pids[@]}" 2>/dev/null || true
    fi
  }

  trap cleanup EXIT INT TERM

  bash scripts/start_api.sh "$@" &
  pids+=("$!")
  started_api=true

  if ! wait_for_api; then
    cleanup
    if [ "$started_api" = true ]; then
      wait "${pids[0]}" 2>/dev/null || true
    fi
    exit 1
  fi

  bash scripts/start_pipeline_worker.sh &
  pids+=("$!")
  bash scripts/start_executor_worker.sh &
  pids+=("$!")
  bash scripts/start_scheduler_worker.sh &
  pids+=("$!")
  bash scripts/start_frontend.sh &
  pids+=("$!")

  wait -n "${pids[@]}"
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
    start_api_workers_and_frontend "$@"
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

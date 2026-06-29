#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

usage() {
  cat <<'USAGE'
Usage: ./run.sh [command]

Commands:
  api             Start the FastAPI backend on APP_PORT (default: 8080)
  init-db         Initialize database tables
  frontend        Start the lightweight static frontend
  pipeline-loop   Start the pipeline worker loop
  executor-loop   Start the executor worker loop
  scheduler-loop  Start the scheduler worker loop
  all             Start API and frontend together
  reserved-vm     Alias for all

If no command is provided, the API and frontend are started.
USAGE
}

start_api_and_frontend() {
  bash scripts/start_api.sh "$@" &
  API_PID=$!

  bash scripts/start_frontend.sh &
  FRONTEND_PID=$!

  cleanup() {
    kill "$API_PID" "$FRONTEND_PID" 2>/dev/null || true
  }

  trap cleanup EXIT INT TERM

  wait "$API_PID"
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

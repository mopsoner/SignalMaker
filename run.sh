#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

usage() {
  cat <<'USAGE'
Usage: ./run.sh [command]

Commands:
  device          Historical Raspberry device mode: remote candle feed + remote executor loops + local monitoring UI
  candle-feed     Start only the Raspberry -> remote SignalMaker candle feed loop
  backfill        Run the historical Raspberry -> remote SignalMaker candle backfill once
  executor        Start only the remote SignalMaker trade/momentum executor loop
  local-api       Start only the local Raspberry Executor monitoring API/UI
  all-local       Start local API + local pipeline + local executor + local scheduler
  api             Alias for local-api
  init-db         Initialize database tables
  frontend        Legacy no-op: frontend is served by the API on APP_PORT
  pipeline-loop   Start the local pipeline worker loop
  executor-loop   Start the local executor worker loop
  scheduler-loop  Start the local scheduler worker loop
  all             Alias for device (historical default)
  reserved-vm     Alias for all-local

If no command is provided, historical Raspberry device mode is started.
USAGE
}

wait_for_api() {
  local port="${APP_PORT:-5000}"
  local health_url="http://127.0.0.1:${port}/healthz"
  local timeout_seconds="${API_STARTUP_TIMEOUT:-300}"
  local check_interval_seconds="${API_STARTUP_CHECK_INTERVAL:-30}"

  if [ "$timeout_seconds" -lt 300 ]; then
    timeout_seconds=300
  fi

  local deadline=$((SECONDS + timeout_seconds))

  echo "Waiting for SignalMaker Raspberry Executor local API at ${health_url} before starting workers/executor..."
  echo "Health check timeout: ${timeout_seconds}s; interval: ${check_interval_seconds}s."
  while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -fsS --max-time 2 "$health_url" >/dev/null 2>&1; then
      echo "SignalMaker Raspberry Executor local API is ready; starting workers/executor."
      return 0
    fi
    local remaining=$((deadline - SECONDS))
    if [ "$remaining" -le 0 ]; then
      break
    fi
    if [ "$remaining" -lt "$check_interval_seconds" ]; then
      sleep "$remaining"
    else
      sleep "$check_interval_seconds"
    fi
  done

  echo "Backend did not become ready at ${health_url} within ${timeout_seconds}s; workers/executor will not start." >&2
  return 1
}

python_cmd() {
  if [ -x ".venv/bin/python" ]; then
    echo ".venv/bin/python"
  else
    echo "python3"
  fi
}

start_api_and_workers() {
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

  wait -n "${pids[@]}"
}

command="${1:-device}"
if [ "$#" -gt 0 ]; then
  shift
fi

case "$command" in
  device|all)
    exec "$(python_cmd)" -m raspberry_executor.run_all_v2 "$@"
    ;;
  candle-feed)
    exec "$(python_cmd)" -m raspberry_executor.candle_auto_feed "$@"
    ;;
  backfill)
    exec "$(python_cmd)" -m raspberry_executor.candle_backfill_4h --run "$@"
    ;;
  executor)
    exec "$(python_cmd)" -c 'from raspberry_executor.run_all_v2 import executor_main; executor_main()'
    ;;
  local-api|api)
    exec bash scripts/start_api.sh "$@"
    ;;
  init-db)
    exec python -m scripts.init_db "$@"
    ;;
  frontend)
    echo "The Raspberry frontend is served by signalmaker-api on http://127.0.0.1:${APP_PORT:-5000}/index.html; no separate frontend server is started."
    exit 0
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
  all-local|reserved-vm)
    start_api_and_workers "$@"
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

#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

usage() {
  cat <<'USAGE'
Usage: ./run.sh [command]

Commands:
  device          Official Raspberry device mode: local API/UI + health wait + run_all_v2 executor bundle
  candle-feed     Start only the Raspberry -> remote SignalMaker candle feed loop
  backfill        Run the historical Raspberry -> remote SignalMaker candle backfill once
  executor        Start only the remote SignalMaker trade/momentum executor loop
  local-api       Start only the local Raspberry Executor monitoring API/UI
  smoke           Run Raspberry Executor Kraken/device smoke checks
  api             Alias for local-api
  init-db         Initialize database tables
  frontend        Legacy no-op: frontend is served by the API on APP_PORT
  pipeline-loop   Start the local pipeline worker loop
  executor-loop   Start the local executor worker loop
  scheduler-loop  Start the local scheduler worker loop
  all             Alias for device (historical default)

If no command is provided, the official Raspberry device mode is started.
USAGE
}

wait_for_api() {
  local port="${EXECUTOR_API_PORT:-${APP_PORT:-8080}}"
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

load_database_url() {
  if [ -n "${DATABASE_URL+x}" ] || [ ! -f ".env" ]; then
    return 0
  fi

  local configured_url
  configured_url="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*(.*)$/\2/p' .env | head -n 1)"
  configured_url="${configured_url%$'\r'}"
  if [[ "$configured_url" == \"*\" && "$configured_url" == *\" ]] || \
     [[ "$configured_url" == \'*\' && "$configured_url" == *\' ]]; then
    configured_url="${configured_url:1:${#configured_url}-2}"
  fi
  if [ -n "$configured_url" ]; then
    export DATABASE_URL="$configured_url"
  fi
}

wait_for_database() {
  load_database_url

  case "${DATABASE_URL:-}" in
    postgres://*|postgresql://*|postgresql+*://*) ;;
    *) return 0 ;;
  esac

  if ! command -v pg_isready >/dev/null 2>&1; then
    echo "DATABASE_URL uses PostgreSQL, but pg_isready is not installed or not in PATH." >&2
    return 1
  fi

  local timeout_seconds="${POSTGRES_STARTUP_TIMEOUT:-60}"
  local check_interval_seconds="${POSTGRES_STARTUP_CHECK_INTERVAL:-2}"
  if ! [[ "$timeout_seconds" =~ ^[1-9][0-9]*$ ]] || ! [[ "$check_interval_seconds" =~ ^[1-9][0-9]*$ ]]; then
    echo "POSTGRES_STARTUP_TIMEOUT and POSTGRES_STARTUP_CHECK_INTERVAL must be positive integers." >&2
    return 1
  fi
  local host_port
  host_port="$("$(python_cmd)" - <<'PY'
import os
from urllib.parse import urlsplit

url = os.environ["DATABASE_URL"]
# libpq does not understand SQLAlchemy's postgresql+driver scheme.
url = "postgresql" + url[url.find(":"):] if url.startswith("postgresql+") else url
parsed = urlsplit(url)
print(parsed.hostname or "localhost", parsed.port or 5432)
PY
)"
  local database_host="${host_port% *}"
  local database_port="${host_port##* }"
  local deadline=$((SECONDS + timeout_seconds))

  echo "Waiting up to ${timeout_seconds}s for PostgreSQL at ${database_host}:${database_port}..."
  while [ "$SECONDS" -lt "$deadline" ]; do
    if pg_isready -h "$database_host" -p "$database_port" >/dev/null 2>&1; then
      echo "PostgreSQL is ready at ${database_host}:${database_port}."
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

  echo "PostgreSQL configured by DATABASE_URL is still unavailable at ${database_host}:${database_port} after ${timeout_seconds}s; SignalMaker will not start." >&2
  return 1
}

start_api_and_device() {
  local pids=()
  local started_api=false

  cleanup() {
    if [ "${#pids[@]}" -gt 0 ]; then
      kill "${pids[@]}" 2>/dev/null || true
    fi
  }

  trap cleanup EXIT INT TERM

  wait_for_database
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

  "$(python_cmd)" -m raspberry_executor.run_all_v2 "$@" &
  pids+=("$!")

  wait -n "${pids[@]}"
}

command="${1:-device}"
if [ "$#" -gt 0 ]; then
  shift
fi

case "$command" in
  device|all)
    start_api_and_device "$@"
    ;;
  candle-feed)
    exec "$(python_cmd)" -m raspberry_executor.candle_auto_feed "$@"
    ;;
  backfill)
    exec "$(python_cmd)" -m raspberry_executor.candle_backfill_4h --run "$@"
    ;;
  smoke)
    exec "$(python_cmd)" -m raspberry_executor.kraken_full_smoke_test "$@"
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
    echo "The Raspberry frontend is served by signalmaker-api on http://127.0.0.1:${EXECUTOR_API_PORT:-${APP_PORT:-8080}}/index.html; no separate frontend server is started."
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
  -h|--help|help)
    usage
    ;;
  *)
    echo "Unknown command: $command" >&2
    usage >&2
    exit 2
    ;;
esac

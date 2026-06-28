#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

ensure_venv() {
  if [ ! -d .venv ] || ! grep -q "include-system-site-packages = true" .venv/pyvenv.cfg 2>/dev/null; then
    echo "Creating Python virtual environment in .venv..."
    rm -rf .venv
    python3 -m venv .venv --system-site-packages
  fi

  # shellcheck disable=SC1091
  source .venv/bin/activate

  if [ ! -f .venv/.requirements-installed ] || [ requirements.txt -nt .venv/.requirements-installed ]; then
    echo "Installing Python dependencies from requirements.txt..."
    if python -m pip install -r requirements.txt; then
      touch .venv/.requirements-installed
    else
      echo "Dependency installation failed; checking whether required modules are already available..." >&2
      python - <<'PY'
import importlib.util
missing = [module for module in ("fastapi", "uvicorn", "sqlalchemy", "pydantic_settings") if importlib.util.find_spec(module) is None]
if missing:
    raise SystemExit("Missing Python modules: " + ", ".join(missing) + ". Run: python -m pip install -r requirements.txt")
PY
      touch .venv/.requirements-installed
    fi
  fi
}

usage() {
  cat <<'USAGE'
Usage: ./run.sh [command]

Commands:
  api             Start the FastAPI backend on APP_PORT (default: 8080)
  init-db         Initialize database tables
  frontend        Start the Vite frontend
  pipeline-loop   Start the pipeline worker loop
  executor-loop   Start the executor worker loop
  scheduler-loop  Start the scheduler worker loop
  all             Start API, workers, and frontend together (no TUI)
  reserved-vm     Alias for all

If no command is provided, all non-TUI services are started.
USAGE
}

command="${1:-all}"
if [ "$#" -gt 0 ]; then
  shift
fi

case "$command" in
  api)
    ensure_venv
    exec bash scripts/start_api.sh "$@"
    ;;
  init-db)
    ensure_venv
    exec python -m scripts.init_db "$@"
    ;;
  frontend)
    exec bash scripts/start_frontend.sh "$@"
    ;;
  pipeline-loop)
    ensure_venv
    exec bash scripts/start_pipeline_worker.sh "$@"
    ;;
  executor-loop)
    ensure_venv
    exec bash scripts/start_executor_worker.sh "$@"
    ;;
  scheduler-loop)
    ensure_venv
    exec bash scripts/start_scheduler_worker.sh "$@"
    ;;
  all|reserved-vm)
    ensure_venv
    exec bash scripts/start_reserved_vm.sh "$@"
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

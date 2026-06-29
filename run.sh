#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

requirements_hash() {
  python3 - <<'PY'
from pathlib import Path
import hashlib
print(hashlib.sha256(Path("requirements.txt").read_bytes()).hexdigest())
PY
}

required_python_modules_present() {
  python - <<'PY'
import importlib.util
missing = [module for module in ("fastapi", "uvicorn", "sqlalchemy", "pydantic_settings") if importlib.util.find_spec(module) is None]
if missing:
    raise SystemExit(1)
PY
}

mark_requirements_installed() {
  requirements_hash > .venv/.requirements-installed
}

ensure_venv() {
  if [ ! -d .venv ] || ! grep -q "include-system-site-packages = true" .venv/pyvenv.cfg 2>/dev/null; then
    echo "Creating Python virtual environment in .venv..."
    rm -rf .venv
    python3 -m venv .venv --system-site-packages
  fi

  # shellcheck disable=SC1091
  source .venv/bin/activate

  local current_hash installed_hash
  current_hash="$(requirements_hash)"
  installed_hash="$(cat .venv/.requirements-installed 2>/dev/null || true)"
  if [ "$current_hash" = "$installed_hash" ]; then
    return 0
  fi

  # Fast path for normal launches: if the runtime modules are already importable,
  # avoid running pip just because timestamps changed after a checkout or copy.
  if [ "${RUN_STRICT_DEPS:-0}" != "1" ] && required_python_modules_present; then
    mark_requirements_installed
    return 0
  fi

  if [ "${RUN_AUTO_INSTALL:-1}" = "0" ]; then
    echo "Python dependencies are not marked as installed. Run: python -m pip install -r requirements.txt" >&2
    exit 1
  fi

  echo "Installing Python dependencies from requirements.txt..."
  if python -m pip install -r requirements.txt; then
    mark_requirements_installed
  else
    echo "Dependency installation failed; checking whether required modules are already available..." >&2
    if required_python_modules_present; then
      mark_requirements_installed
    else
      echo "Missing Python modules. Run: python -m pip install -r requirements.txt" >&2
      exit 1
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

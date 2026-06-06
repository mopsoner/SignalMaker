#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
  python3 -m venv .venv --system-site-packages
fi

source .venv/bin/activate
.venv/bin/pip install -q -r requirements.txt

if [ -n "${DATABASE_URL:-}" ]; then
  # Convert postgresql:// or postgres:// -> postgresql+psycopg:// while keeping all other params intact
  export DATABASE_URL="${DATABASE_URL/postgres:\/\//postgresql+psycopg:\/\/}"
  export DATABASE_URL="${DATABASE_URL/postgresql:\/\//postgresql+psycopg:\/\/}"
elif [ -n "${PGHOST:-}" ] && [ -n "${PGUSER:-}" ] && [ -n "${PGPASSWORD:-}" ] && [ -n "${PGDATABASE:-}" ]; then
  _SSLMODE="${PGSSLMODE:-require}"
  export DATABASE_URL="postgresql+psycopg://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT:-5432}/${PGDATABASE}?sslmode=${_SSLMODE}"
fi

MODE="${1:-api}"

case "$MODE" in
  api)
    uvicorn app.main:app --host 0.0.0.0 --port ${APP_PORT:-8080}
    ;;
  dev)
    uvicorn app.main:app --host 0.0.0.0 --port ${APP_PORT:-8080} --reload
    ;;
  init-db)
    python -m scripts.init_db
    ;;
  pipeline-once)
    python - <<'PY'
from app.db.session import SessionLocal
from app.services.pipeline_service import PipelineService

db = SessionLocal()
try:
    print(PipelineService(db).run_once(limit=5))
finally:
    db.close()
PY
    ;;
  executor-once)
    python - <<'PY'
from app.db.session import SessionLocal
from app.services.executor_service import ExecutorService

db = SessionLocal()
try:
    print(ExecutorService(db).execute_open_candidates(limit=10, quantity=1.0))
finally:
    db.close()
PY
    ;;
  momentum-executor-once)
    python - <<'PY'
from app.db.session import SessionLocal
from app.services.momentum_executor_service import MomentumExecutorService

db = SessionLocal()
try:
    print(MomentumExecutorService(db).run_once(force=True))
finally:
    db.close()
PY
    ;;
  pipeline-loop)
    python -m scripts.run_pipeline_loop
    ;;
  executor-loop)
    python -m scripts.run_executor_loop
    ;;
  scheduler-loop)
    python -m scripts.run_scheduler_loop
    ;;
  momentum-executor-loop)
    python -m scripts.run_momentum_executor_loop
    ;;
  momentum-decision-loop)
    python -m raspberry_executor.momentum_decision_feed
    ;;
  momentum-executor-tui)
    python -m scripts.run_momentum_executor_tui
    ;;
  *)
    echo "Usage: bash run.sh [api|dev|init-db|pipeline-once|executor-once|momentum-executor-once|pipeline-loop|executor-loop|scheduler-loop|momentum-executor-loop|momentum-decision-loop|momentum-executor-tui]"
    exit 1
    ;;
esac

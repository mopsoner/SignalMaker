#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
  python3 -m venv .venv --system-site-packages
fi

source .venv/bin/activate
.venv/bin/pip install -q -r requirements.txt

if [ -n "${DATABASE_URL:-}" ]; then
  export DATABASE_URL="${DATABASE_URL/postgres:\/\//postgresql+psycopg:\/\/}"
  export DATABASE_URL="${DATABASE_URL/postgresql:\/\//postgresql+psycopg:\/\/}"
elif [ -n "${PGHOST:-}" ] && [ -n "${PGUSER:-}" ] && [ -n "${PGPASSWORD:-}" ] && [ -n "${PGDATABASE:-}" ]; then
  _SSLMODE="${PGSSLMODE:-require}"
  export DATABASE_URL="postgresql+psycopg://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT:-5432}/${PGDATABASE}?sslmode=${_SSLMODE}"
fi

mkdir -p .runtime logs

python -m scripts.init_db

python -m scripts.run_pipeline_loop >> logs/pipeline.log 2>&1 &
echo $! > .runtime/pipeline.pid

python -m scripts.run_executor_loop >> logs/executor.log 2>&1 &
echo $! > .runtime/executor.pid

python -m scripts.run_scheduler_loop >> logs/scheduler.log 2>&1 &
echo $! > .runtime/scheduler.pid

echo "Workers started — launching API on port ${APP_PORT:-5000}"
exec uvicorn app.main:app --host 0.0.0.0 --port ${APP_PORT:-5000}

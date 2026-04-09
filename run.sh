#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
pip install -q --upgrade pip
pip install -q -r requirements.txt

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
  *)
    echo "Usage: bash run.sh [api|dev|init-db]"
    exit 1
    ;;
esac

#!/usr/bin/env bash
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi

. .venv/bin/activate

if [ ! -f .deps_ok ]; then
  python -m pip install --upgrade pip setuptools wheel
  pip install -r requirements-raspberry.txt
  touch .deps_ok
fi

if [ ! -f .env ]; then
  cp .env.raspberry.example .env
fi

python -m raspberry_executor.install_sqlite
exec python -m raspberry_executor.run_all_v2

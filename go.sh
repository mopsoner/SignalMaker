#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

if [ -d .git ]; then
  git pull --ff-only || true
fi

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

echo "Initializing SQLite database..."
python -m raspberry_executor.install_sqlite

echo "Running Raspberry Binance smoke test..."
python -m raspberry_executor.binance_smoke_test

echo "Running Raspberry quote assets smoke test..."
python -m raspberry_executor.quote_assets_smoke_test

echo "Web UI: http://$(hostname -I | awk '{print $1}'):8090"
echo "Admin:  http://$(hostname -I | awk '{print $1}'):8090/admin"
echo "Logs:   http://$(hostname -I | awk '{print $1}'):8090/logs"
echo ""

python -m raspberry_executor.run_all

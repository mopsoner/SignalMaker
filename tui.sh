#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

bash scripts/self_update.sh || true

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi

. .venv/bin/activate

if [ ! -f .deps_ok ]; then
  python -m pip install --upgrade pip setuptools wheel
  pip install -r requirements-raspberry.txt
  touch .deps_ok
fi

export TERM="${TERM:-linux}"
python -m raspberry_executor.tui_safe_dashboard

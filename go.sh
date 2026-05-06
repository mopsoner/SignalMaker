#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi

. .venv/bin/activate

if [ ! -f .deps_ok ]; then
  pip install -r requirements-raspberry.txt
  touch .deps_ok
fi

if [ ! -f .env ]; then
  cp .env.raspberry.example .env
  echo "Edit .env first: nano .env"
  exit 0
fi

python -m raspberry_executor.main

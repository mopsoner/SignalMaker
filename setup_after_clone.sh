#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PYTHON_BIN="${PYTHON_BIN:-python3}"

printf '\n== SignalMaker Raspberry Executor setup ==\n\n'

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo 'Python 3 is required but was not found.'
  exit 1
fi

if [ ! -d ".venv" ]; then
  echo 'Creating Python virtual environment...'
  "$PYTHON_BIN" -m venv .venv
fi

. .venv/bin/activate

echo 'Installing Python dependencies...'
python -m pip install --upgrade pip
pip install -r requirements-raspberry.txt

if [ ! -f ".env" ]; then
  cp .env.raspberry.example .env
  echo 'Created .env from .env.raspberry.example'
else
  echo '.env already exists, keeping it unchanged.'
fi

printf '\nSetup complete.\n'
printf 'Next step: edit .env with your SignalMaker URL and Binance keys.\n'
printf 'Command: nano .env\n\n'
printf 'Then run:\n'
printf '. .venv/bin/activate && python -m raspberry_executor.main\n\n'
printf 'Keep DRY_RUN=true for first tests.\n'

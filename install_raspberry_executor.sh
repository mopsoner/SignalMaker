#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$HOME/SignalMaker}"
REPO_URL="${REPO_URL:-https://github.com/mopsoner/SignalMaker.git}"
BRANCH="${BRANCH:-raspberry/executor-app}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

printf '\n== SignalMaker Raspberry Executor installer ==\n'
printf 'App dir: %s\n' "$APP_DIR"
printf 'Branch : %s\n\n' "$BRANCH"

if ! command -v git >/dev/null 2>&1; then
  echo 'Installing git...'
  sudo apt-get update
  sudo apt-get install -y git
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo 'Python 3 is required but was not found.'
  exit 1
fi

if [ ! -d "$APP_DIR/.git" ]; then
  git clone -b "$BRANCH" "$REPO_URL" "$APP_DIR"
else
  cd "$APP_DIR"
  git fetch origin "$BRANCH"
  git checkout "$BRANCH"
  git pull origin "$BRANCH"
fi

cd "$APP_DIR"

if [ ! -d ".venv" ]; then
  "$PYTHON_BIN" -m venv .venv
fi

. .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements-raspberry.txt

if [ ! -f ".env" ]; then
  cp .env.raspberry.example .env
  echo ''
  echo 'Created .env from .env.raspberry.example'
fi

printf '\nInstall complete.\n'
printf 'Edit config now with: nano %s/.env\n' "$APP_DIR"
printf 'Run manually with: cd %s && . .venv/bin/activate && python -m raspberry_executor.main\n' "$APP_DIR"
printf '\nKeep DRY_RUN=true for first tests.\n'

#!/usr/bin/env bash
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"
if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi
exec python main.py

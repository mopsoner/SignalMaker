#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/home/pi/Desktop/SignalMaker}"
BRANCH="${BRANCH:-raspberry/executor-app}"
LOG_FILE="${PROJECT_DIR}/logs/one_click_update.log"

mkdir -p "${PROJECT_DIR}/logs"
exec > >(tee -a "$LOG_FILE") 2>&1

printf '\n=== SignalMaker one-click update %s ===\n' "$(date -Is)"

cd "$PROJECT_DIR"

echo "Pull latest code..."
git fetch origin
git checkout "$BRANCH"
git pull origin "$BRANCH"

if [ -f scripts/patch_ui_contract_endpoints.py ]; then
  echo "Apply shared UI endpoint patch..."
  python3 scripts/patch_ui_contract_endpoints.py
fi

echo "Run full Raspberry fix script..."
PROJECT_DIR="$PROJECT_DIR" bash scripts/fix_raspberry_executor_service.sh

if [ -f scripts/patch_ui_contract_endpoints.py ]; then
  echo "Re-apply shared UI endpoint patch after fix script..."
  python3 scripts/patch_ui_contract_endpoints.py
  echo "Restart service after UI patch..."
  sudo systemctl restart raspberry-executor || true
fi

echo "Done. Log: $LOG_FILE"

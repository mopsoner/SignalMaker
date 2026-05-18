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

echo "Apply local Raspberry patches..."
for patch in scripts/patch_*.py; do
  if [ -f "$patch" ]; then
    echo "Apply $patch"
    python3 "$patch"
  fi
done

echo "Run full Raspberry fix script..."
PROJECT_DIR="$PROJECT_DIR" bash scripts/fix_raspberry_executor_service.sh

echo "Re-apply local Raspberry patches after fix script..."
for patch in scripts/patch_*.py; do
  if [ -f "$patch" ]; then
    echo "Apply $patch"
    python3 "$patch"
  fi
done

echo "Restart service after patches..."
sudo systemctl restart raspberry-executor || true

echo "Done. Log: $LOG_FILE"

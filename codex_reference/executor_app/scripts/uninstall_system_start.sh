#!/usr/bin/env bash
set -euo pipefail

EXECUTOR_SERVICE="raspberry-executor.service"
LEGACY_BOT_SERVICE="signalmaker-bot.service"
TUI_SERVICE="signalmaker-tui.service"

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemd not found; nothing to uninstall"
  exit 0
fi

if [ "$(id -u)" -ne 0 ]; then
  echo "Uninstalling services with sudo..."
  exec sudo bash "$0"
fi

for service in "$TUI_SERVICE" "$EXECUTOR_SERVICE" "$LEGACY_BOT_SERVICE"; do
  systemctl stop "$service" >/dev/null 2>&1 || true
  systemctl disable "$service" >/dev/null 2>&1 || true
  rm -f "/etc/systemd/system/$service"
done

systemctl daemon-reload
systemctl reset-failed >/dev/null 2>&1 || true

echo "Removed: $EXECUTOR_SERVICE, $TUI_SERVICE (and legacy $LEGACY_BOT_SERVICE if present)"
echo "Project files, .env and SQLite database were not deleted."

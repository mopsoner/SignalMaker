#!/usr/bin/env bash
set -euo pipefail

BOT_SERVICE="signalmaker-bot.service"
TUI_SERVICE="signalmaker-tui.service"
TTY_NAME="${TTY_NAME:-tty1}"

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemd not found; nothing to uninstall"
  exit 0
fi

if [ "$(id -u)" -ne 0 ]; then
  echo "Uninstalling services with sudo..."
  exec sudo TTY_NAME="$TTY_NAME" bash "$0"
fi

for service in "$TUI_SERVICE" "$BOT_SERVICE"; do
  systemctl stop "$service" >/dev/null 2>&1 || true
  systemctl disable "$service" >/dev/null 2>&1 || true
  rm -f "/etc/systemd/system/$service"
done

# Restore the normal login prompt on tty1.
systemctl enable "getty@${TTY_NAME}.service" >/dev/null 2>&1 || true
systemctl start "getty@${TTY_NAME}.service" >/dev/null 2>&1 || true

systemctl daemon-reload
systemctl reset-failed >/dev/null 2>&1 || true

echo "Removed: $BOT_SERVICE, $TUI_SERVICE"
echo "Restored: getty@${TTY_NAME}.service"
echo "Project files, .env and SQLite database were not deleted."

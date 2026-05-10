#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_USER="${SUDO_USER:-$(id -un)}"
TTY_NAME="${TTY_NAME:-tty1}"
BOT_SERVICE="signalmaker-bot.service"
TUI_SERVICE="signalmaker-tui.service"

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemd not found; skipping service install"
  exit 0
fi

if [ "$(id -u)" -ne 0 ]; then
  echo "Installing services with sudo..."
  exec sudo RUN_USER="$RUN_USER" TTY_NAME="$TTY_NAME" bash "$0"
fi

cat > "/etc/systemd/system/${BOT_SERVICE}" <<EOF
[Unit]
Description=SignalMaker Raspberry bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_USER}
WorkingDirectory=${APP_DIR}
ExecStart=/bin/bash ${APP_DIR}/run_bot_service.sh
Restart=always
RestartSec=8
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

cat > "/etc/systemd/system/${TUI_SERVICE}" <<EOF
[Unit]
Description=SignalMaker Raspberry fullscreen TUI
After=${BOT_SERVICE} network-online.target
Wants=${BOT_SERVICE} network-online.target
Conflicts=getty@${TTY_NAME}.service
After=getty@${TTY_NAME}.service

[Service]
Type=simple
User=${RUN_USER}
WorkingDirectory=${APP_DIR}
Environment=TERM=linux
TTYPath=/dev/${TTY_NAME}
StandardInput=tty
StandardOutput=tty
StandardError=journal
ExecStart=/bin/bash -lc 'clear; exec ${APP_DIR}/tui.sh'
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "${BOT_SERVICE}"
systemctl enable "${TUI_SERVICE}"

# Stop login prompt on tty1 so the TUI owns the screen.
systemctl disable "getty@${TTY_NAME}.service" >/dev/null 2>&1 || true

echo "Installed and enabled: ${BOT_SERVICE}, ${TUI_SERVICE}"
echo "Reboot to start automatically, or run:"
echo "  sudo systemctl start ${BOT_SERVICE}"
echo "  sudo systemctl start ${TUI_SERVICE}"

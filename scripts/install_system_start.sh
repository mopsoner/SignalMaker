#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_USER="${SUDO_USER:-$(id -un)}"
TTY_NAME="${TTY_NAME:-tty1}"
EXECUTOR_SERVICE="raspberry-executor.service"
TUI_SERVICE="signalmaker-tui.service"

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemd not found; skipping service install"
  exit 0
fi

if [ "$(id -u)" -ne 0 ]; then
  echo "Installing services with sudo..."
  exec sudo RUN_USER="$RUN_USER" TTY_NAME="$TTY_NAME" bash "$0"
fi

cat > "/etc/systemd/system/${EXECUTOR_SERVICE}" <<EOF
[Unit]
Description=SignalMaker Raspberry Executor
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_USER}
WorkingDirectory=${APP_DIR}
ExecStart=/bin/bash ${APP_DIR}/run.sh device
Restart=always
RestartSec=8
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

cat > "/etc/systemd/system/${TUI_SERVICE}" <<EOF
[Unit]
Description=SignalMaker Raspberry fullscreen TUI on ${TTY_NAME}
After=${EXECUTOR_SERVICE} network-online.target systemd-user-sessions.service
Wants=${EXECUTOR_SERVICE} network-online.target
Conflicts=getty@${TTY_NAME}.service

[Service]
Type=simple
User=${RUN_USER}
WorkingDirectory=${APP_DIR}
Environment=TERM=xterm-256color
Environment=PYTHONUNBUFFERED=1
Environment=DISPLAY=
TTYPath=/dev/${TTY_NAME}
TTYReset=yes
TTYVHangup=yes
TTYVTDisallocate=yes
StandardInput=tty
StandardOutput=tty
StandardError=journal
# ExecStartPre prefixed with + runs as root even though the service runs as RUN_USER.
# This is needed to switch the physical HDMI console to tty1 on boot.
ExecStartPre=+/bin/sleep 5
ExecStartPre=+/usr/bin/chvt ${TTY_NAME#tty}
ExecStartPre=+/usr/bin/setterm -blank 0 -powerdown 0 -powersave off -term linux -store
ExecStartPre=+/bin/sh -c 'printf "\033c" > /dev/${TTY_NAME}'
ExecStart=/bin/bash -lc 'exec ${APP_DIR}/tui.sh'
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "${EXECUTOR_SERVICE}"
systemctl enable "${TUI_SERVICE}"

# Stop login prompt on selected tty so the TUI owns the screen.
systemctl disable "getty@${TTY_NAME}.service" >/dev/null 2>&1 || true
systemctl stop "getty@${TTY_NAME}.service" >/dev/null 2>&1 || true

echo "Installed and enabled: ${EXECUTOR_SERVICE}, ${TUI_SERVICE}"
echo "TTY: /dev/${TTY_NAME}"
echo "Reboot to start automatically, or run:"
echo "  sudo systemctl restart ${EXECUTOR_SERVICE}"
echo "  sudo systemctl restart ${TUI_SERVICE}"
echo "Diagnostics:"
echo "  systemctl status ${TUI_SERVICE}"
echo "  journalctl -u ${TUI_SERVICE} -n 120 --no-pager"
echo "  sudo chvt ${TTY_NAME#tty}"

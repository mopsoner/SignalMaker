#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_USER="${SUDO_USER:-$(id -un)}"
EXECUTOR_SERVICE="raspberry-executor.service"

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemd not found; skipping service install"
  exit 0
fi

if [ "$(id -u)" -ne 0 ]; then
  echo "Installing service with sudo..."
  exec sudo RUN_USER="$RUN_USER" bash "$0"
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
ExecStart=/bin/bash ${APP_DIR}/run.sh
Restart=always
RestartSec=8
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "${EXECUTOR_SERVICE}"

echo "Installed and enabled: ${EXECUTOR_SERVICE}"
echo "Reboot to start automatically, or run:"
echo "  sudo systemctl restart ${EXECUTOR_SERVICE}"
echo "Diagnostics:"
echo "  systemctl status ${EXECUTOR_SERVICE}"
echo "  journalctl -u ${EXECUTOR_SERVICE} -n 120 --no-pager"

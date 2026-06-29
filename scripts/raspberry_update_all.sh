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
    python3 "$patch" || true
  fi
done

echo "Run full Raspberry fix script..."
PROJECT_DIR="$PROJECT_DIR" bash scripts/fix_raspberry_executor_service.sh

echo "Re-apply local Raspberry patches after fix script..."
for patch in scripts/patch_*.py; do
  if [ -f "$patch" ]; then
    echo "Apply $patch"
    python3 "$patch" || true
  fi
done

echo "Install robust tty1 dashboard service..."
if [ -f systemd/signalmaker-tui.service ]; then
  sudo systemctl disable --now getty@tty1.service || true
  sudo cp systemd/signalmaker-tui.service /etc/systemd/system/signalmaker-tui.service
  sudo chmod 644 /etc/systemd/system/signalmaker-tui.service
  sudo systemctl daemon-reload
  sudo systemctl enable signalmaker-tui.service
else
  echo "WARN: systemd/signalmaker-tui.service not found"
fi

echo "Install graphical browser kiosk fallback..."
if [ -f scripts/start_kiosk_browser.sh ]; then
  chmod +x scripts/start_kiosk_browser.sh || true
  sudo tee /etc/systemd/system/signalmaker-kiosk.service >/dev/null <<'EOF'
[Unit]
Description=SignalMaker Browser Kiosk
After=graphical.target raspberry-executor.service
Wants=graphical.target raspberry-executor.service

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi/Desktop/SignalMaker
Environment=DISPLAY=:0
Environment=XAUTHORITY=/home/pi/.Xauthority
Environment=SIGNALMAKER_KIOSK_URL=http://127.0.0.1:5000/positions.html
ExecStart=/bin/bash /home/pi/Desktop/SignalMaker/scripts/start_kiosk_browser.sh
Restart=always
RestartSec=10

[Install]
WantedBy=graphical.target
EOF
  sudo chmod 644 /etc/systemd/system/signalmaker-kiosk.service
  sudo systemctl daemon-reload
  sudo systemctl enable signalmaker-kiosk.service || true
else
  echo "WARN: scripts/start_kiosk_browser.sh not found"
fi

echo "Restart services after patches..."
sudo systemctl restart raspberry-executor || true
sudo systemctl restart signalmaker-tui.service || true
sudo systemctl restart signalmaker-kiosk.service || true
sudo chvt 1 || true

echo "TUI service status:"
sudo systemctl status signalmaker-tui.service --no-pager || true

echo "Kiosk service status:"
sudo systemctl status signalmaker-kiosk.service --no-pager || true

echo "Done. Log: $LOG_FILE"

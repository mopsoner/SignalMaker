#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; hourly=false; enable=false
for arg in "$@"; do [[ "$arg" == --hourly ]] && hourly=true; [[ "$arg" == --enable-timer ]] && enable=true; done
SERVICE=/etc/systemd/system/signalmaker-ibkr-feeder.service; TIMER=/etc/systemd/system/signalmaker-ibkr-feeder.timer
for file in "$SERVICE" "$TIMER"; do [[ -e "$file" ]] && sudo cp "$file" "$file.bak.$(date +%Y%m%d%H%M%S)"; done
sudo tee "$SERVICE" >/dev/null <<EOF
[Unit]
Description=SignalMaker local IBKR candle feeder
After=network-online.target
Wants=network-online.target
[Service]
Type=oneshot
WorkingDirectory=$ROOT
ExecStart=$ROOT/.venv/bin/python scripts/ibkr_feeder.py
EnvironmentFile=-$ROOT/.env
Restart=on-failure
RestartSec=60
[Install]
WantedBy=multi-user.target
EOF
CALENDAR=daily; $hourly && CALENDAR=hourly
sudo tee "$TIMER" >/dev/null <<EOF
[Unit]
Description=Schedule SignalMaker IBKR feeder
[Timer]
OnCalendar=$CALENDAR
Persistent=true
Unit=signalmaker-ibkr-feeder.service
[Install]
WantedBy=timers.target
EOF
sudo systemctl daemon-reload
$enable && sudo systemctl enable --now signalmaker-ibkr-feeder.timer
echo 'systemctl status signalmaker-ibkr-feeder.service'
echo 'journalctl -u signalmaker-ibkr-feeder.service -n 200 -f'

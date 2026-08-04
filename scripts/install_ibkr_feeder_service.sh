#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; hourly=false; enable=false; universe="${FEED_UNIVERSE:-}"
for arg in "$@"; do [[ "$arg" == --hourly ]] && hourly=true; [[ "$arg" == --enable-timer ]] && enable=true; done
while [[ $# -gt 0 ]]; do [[ "$1" == --universe ]] && { universe="$2"; shift 2; continue; }; shift; done
[[ "$universe" == "Europe Stocks" || "$universe" == "Europe ETF" ]] || { echo 'Use --universe "Europe Stocks" or --universe "Europe ETF"' >&2; exit 2; }
slug="$(tr '[:upper:] ' '[:lower:]_' <<<"$universe")"; SERVICE="/etc/systemd/system/executor-feeder-$slug.service"; TIMER="/etc/systemd/system/executor-feeder-$slug.timer"
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
Environment="FEED_UNIVERSE=$universe"
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
Unit=executor-feeder-$slug.service
[Install]
WantedBy=timers.target
EOF
sudo systemctl daemon-reload
$enable && sudo systemctl enable --now signalmaker-ibkr-feeder.timer
echo "systemctl status executor-feeder-$slug.service"
echo "journalctl -u executor-feeder-$slug.service -n 200 -f"

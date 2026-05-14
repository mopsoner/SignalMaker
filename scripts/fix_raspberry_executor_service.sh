#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/home/pi/SignalMaker}"
BRANCH="${BRANCH:-raspberry/executor-app}"
SERVICE_NAME="${SERVICE_NAME:-raspberry-executor}"
SERVICE_FILE="systemd/raspberry-executor.service"
SYSTEMD_TARGET="/etc/systemd/system/${SERVICE_NAME}.service"

info() { printf '\n\033[1;36m%s\033[0m\n' "$*"; }
warn() { printf '\n\033[1;33mWARN: %s\033[0m\n' "$*"; }
fail() { printf '\n\033[1;31mERROR: %s\033[0m\n' "$*"; exit 1; }

info "SignalMaker Raspberry Executor Fix"

if [ ! -d "$PROJECT_DIR/.git" ]; then
  fail "Git project not found at $PROJECT_DIR. Set PROJECT_DIR=/path/to/SignalMaker if needed."
fi

cd "$PROJECT_DIR"

info "1) Fetch latest code"
git fetch origin

info "2) Checkout branch: $BRANCH"
git checkout "$BRANCH"

info "3) Pull latest commits"
git pull origin "$BRANCH"

info "4) Verify systemd service file"
[ -f "$SERVICE_FILE" ] || fail "Missing $SERVICE_FILE after pull"

if ! grep -q "raspberry_executor.run_all_v2" "$SERVICE_FILE"; then
  fail "$SERVICE_FILE does not point to raspberry_executor.run_all_v2"
fi

info "5) Install updated systemd service"
sudo cp "$SERVICE_FILE" "$SYSTEMD_TARGET"
sudo chmod 644 "$SYSTEMD_TARGET"

info "6) Reload systemd"
sudo systemctl daemon-reload

info "7) Enable service on boot"
sudo systemctl enable "$SERVICE_NAME"

info "8) Restart service"
sudo systemctl restart "$SERVICE_NAME"

info "9) Wait for startup"
sleep 4

info "10) Service status"
sudo systemctl status "$SERVICE_NAME" --no-pager || true

info "11) ExecStart check"
sudo systemctl cat "$SERVICE_NAME" | grep ExecStart || true

info "12) Recent relevant logs"
journalctl -u "$SERVICE_NAME" -n 160 --no-pager | grep -E "dry_run|candidate status sync|execution mode|local 360 dashboard|candle feed|order monitor|Raspberry margin executor started|raspberry_executor.run_all_v2" || true

info "13) Quick runtime checks"
if sudo systemctl cat "$SERVICE_NAME" | grep -q "raspberry_executor.run_all_v2"; then
  echo "OK: systemd uses raspberry_executor.run_all_v2"
else
  warn "systemd does not show raspberry_executor.run_all_v2"
fi

if journalctl -u "$SERVICE_NAME" -n 200 --no-pager | grep -q "candidate status sync thread started"; then
  echo "OK: candidate status sync thread started"
else
  warn "candidate status sync thread not seen yet in recent logs"
fi

if journalctl -u "$SERVICE_NAME" -n 200 --no-pager | grep -q "order monitor thread started"; then
  echo "OK: order monitor thread started"
else
  warn "order monitor thread not seen yet in recent logs"
fi

cat <<'EOF'

Done.

Expected service line:
ExecStart=/home/pi/SignalMaker/.venv/bin/python -m raspberry_executor.run_all_v2

If TUI still shows: Dry run global=true
1) Open SignalMaker Admin
2) Set Live trading enabled = true
3) Set Use Binance testnet = false for real Binance
4) Save
5) Run: sudo systemctl restart raspberry-executor

EOF

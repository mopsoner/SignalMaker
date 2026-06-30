#!/usr/bin/env bash
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$APP_DIR"

APP_PORT="${APP_PORT:-5000}"
URL="${SIGNALMAKER_KIOSK_URL:-http://127.0.0.1:${APP_PORT}/index.html}"
HEALTH_URL="${SIGNALMAKER_HEALTH_URL:-http://127.0.0.1:${APP_PORT}/healthz}"

BROWSER=""
for candidate in chromium-browser chromium google-chrome google-chrome-stable; do
  if command -v "$candidate" >/dev/null 2>&1; then
    BROWSER="$candidate"
    break
  fi
done

if [ -z "$BROWSER" ]; then
  cat >&2 <<'EOF'
Chromium/Chrome is not installed; kiosk mode cannot start.
Install it with one of these Raspberry Pi OS commands:
  sudo apt install -y chromium-browser
  sudo apt install -y chromium
EOF
  exit 1
fi

printf 'Waiting for SignalMaker API at %s' "$HEALTH_URL"
ready=0
for _ in $(seq 1 60); do
  if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
    ready=1
    break
  fi
  printf '.'
  sleep 1
done
printf '\n'
if [ "$ready" != "1" ]; then
  echo "SignalMaker API did not become ready after 60 seconds: $HEALTH_URL" >&2
  exit 1
fi

export DISPLAY="${DISPLAY:-:0}"
if [ -z "${XAUTHORITY:-}" ] && [ -f "$HOME/.Xauthority" ]; then
  export XAUTHORITY="$HOME/.Xauthority"
fi
xset s off >/dev/null 2>&1 || true
xset -dpms >/dev/null 2>&1 || true
xset s noblank >/dev/null 2>&1 || true

exec "$BROWSER" \
  --kiosk \
  --noerrdialogs \
  --disable-infobars \
  --disable-session-crashed-bubble \
  --disable-translate \
  --check-for-update-interval=31536000 \
  --autoplay-policy=no-user-gesture-required \
  "$URL"

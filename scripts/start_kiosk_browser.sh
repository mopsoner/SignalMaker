#!/usr/bin/env bash
set -euo pipefail

URL="${SIGNALMAKER_KIOSK_URL:-http://127.0.0.1:8090/positions}"

export DISPLAY="${DISPLAY:-:0}"
export XAUTHORITY="${XAUTHORITY:-/home/pi/.Xauthority}"

# Wait for local web dashboard.
for i in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:8090/api/ui/status" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

# Keep screen awake in desktop sessions.
xset s off || true
xset -dpms || true
xset s noblank || true

BROWSER=""
for candidate in chromium-browser chromium google-chrome firefox-esr firefox; do
  if command -v "$candidate" >/dev/null 2>&1; then
    BROWSER="$candidate"
    break
  fi
done

if [ -z "$BROWSER" ]; then
  echo "No browser found for kiosk mode" >&2
  exit 1
fi

if [[ "$BROWSER" == chromium* || "$BROWSER" == google-chrome ]]; then
  exec "$BROWSER" \
    --noerrdialogs \
    --disable-infobars \
    --disable-session-crashed-bubble \
    --disable-features=TranslateUI \
    --check-for-update-interval=31536000 \
    --kiosk "$URL"
fi

exec "$BROWSER" --kiosk "$URL"

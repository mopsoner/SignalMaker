#!/usr/bin/env bash
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DESKTOP_DIR="${HOME}/Desktop"
LAUNCHER_SRC="${APP_DIR}/deploy/desktop/signalmaker-momentum-tui.desktop"
LAUNCHER_DST="${DESKTOP_DIR}/signalmaker-momentum-tui.desktop"

mkdir -p "$DESKTOP_DIR"
cp "$LAUNCHER_SRC" "$LAUNCHER_DST"
chmod +x "$LAUNCHER_DST"
chmod +x "${APP_DIR}/scripts/start_momentum_tui.sh"
chmod +x "${APP_DIR}/scripts/momentum_tui.py"

echo "Momentum TUI desktop button installed: $LAUNCHER_DST"

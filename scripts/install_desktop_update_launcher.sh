#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/home/pi/Desktop/SignalMaker}"
DESKTOP_DIR="${DESKTOP_DIR:-/home/pi/Desktop}"
LAUNCHER_PATH="$DESKTOP_DIR/SignalMaker Update.desktop"

mkdir -p "$DESKTOP_DIR"
cd "$PROJECT_DIR"

cat > "$LAUNCHER_PATH" <<EOF
[Desktop Entry]
Type=Application
Name=SignalMaker Update
Comment=Pull, patch and restart SignalMaker Raspberry executor
Exec=lxterminal -e "bash -lc 'cd $PROJECT_DIR && bash scripts/raspberry_update_all.sh; echo; echo Done. Press Enter to close.; read'"
Icon=utilities-terminal
Terminal=false
Categories=Utility;
EOF

chmod +x "$LAUNCHER_PATH"
chmod +x "$PROJECT_DIR/scripts/raspberry_update_all.sh" || true
chmod +x "$PROJECT_DIR/scripts/fix_raspberry_executor_service.sh" || true

echo "Installed desktop launcher: $LAUNCHER_PATH"

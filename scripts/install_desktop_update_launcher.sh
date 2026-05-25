#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-$HOME/Desktop/SignalMaker}"
DESKTOP_DIR="${DESKTOP_DIR:-$HOME/Desktop}"
BRANCH="${BRANCH:-raspberry/executor-app}"
UPDATE_LAUNCHER_PATH="$DESKTOP_DIR/SignalMaker Update Executor.desktop"
TUI_LAUNCHER_PATH="$DESKTOP_DIR/SignalMaker TUI.desktop"

mkdir -p "$DESKTOP_DIR"
cd "$PROJECT_DIR"

cat > "$UPDATE_LAUNCHER_PATH" <<EOF
[Desktop Entry]
Type=Application
Name=SignalMaker Update Executor
Comment=Pull latest Raspberry executor branch and restart services
Exec=lxterminal -e "bash -lc 'set -e; cd $PROJECT_DIR; echo Updating SignalMaker Raspberry executor...; git fetch origin; git checkout $BRANCH; git pull origin $BRANCH; chmod +x tui.sh scripts/*.sh 2>/dev/null || true; echo Restarting services...; sudo systemctl restart raspberry-executor.service || sudo systemctl restart raspberry-executor; sudo systemctl restart signalmaker-tui.service || true; echo; echo Update complete. Press Enter to close.; read'"
Icon=system-software-update
Terminal=false
Categories=Utility;
EOF

cat > "$TUI_LAUNCHER_PATH" <<EOF
[Desktop Entry]
Type=Application
Name=SignalMaker TUI
Comment=Open the SignalMaker Raspberry terminal dashboard
Exec=lxterminal -e "bash -lc 'cd $PROJECT_DIR; chmod +x tui.sh 2>/dev/null || true; ./tui.sh'"
Icon=utilities-terminal
Terminal=false
Categories=Utility;
EOF

chmod +x "$UPDATE_LAUNCHER_PATH"
chmod +x "$TUI_LAUNCHER_PATH"
chmod +x "$PROJECT_DIR/tui.sh" || true
chmod +x "$PROJECT_DIR/scripts/raspberry_update_all.sh" || true
chmod +x "$PROJECT_DIR/scripts/fix_raspberry_executor_service.sh" || true

echo "Installed desktop launcher: $UPDATE_LAUNCHER_PATH"
echo "Installed desktop launcher: $TUI_LAUNCHER_PATH"

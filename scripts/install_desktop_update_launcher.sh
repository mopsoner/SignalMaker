#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-$HOME/Desktop/SignalMaker}"
DESKTOP_DIR="${DESKTOP_DIR:-$HOME/Desktop}"
BRANCH="${BRANCH:-raspberry/executor-app}"
UPDATE_LAUNCHER_PATH="$DESKTOP_DIR/SignalMaker Update and Restart.desktop"
TUI_LAUNCHER_PATH="$DESKTOP_DIR/SignalMaker Open TUI.desktop"
OLD_UPDATE_LAUNCHER_PATH="$DESKTOP_DIR/SignalMaker Update.desktop"
OLD_UPDATE_EXECUTOR_LAUNCHER_PATH="$DESKTOP_DIR/SignalMaker Update Executor.desktop"
OLD_TUI_LAUNCHER_PATH="$DESKTOP_DIR/SignalMaker TUI.desktop"

mkdir -p "$DESKTOP_DIR"
cd "$PROJECT_DIR"

# Remove older launchers so the desktop does not keep a stale icon that starts
# a dashboard/kiosk instead of the terminal TUI.
rm -f "$OLD_UPDATE_LAUNCHER_PATH" "$OLD_UPDATE_EXECUTOR_LAUNCHER_PATH" "$OLD_TUI_LAUNCHER_PATH"

cat > "$UPDATE_LAUNCHER_PATH" <<EOF
[Desktop Entry]
Type=Application
Name=SignalMaker Update and Restart
Comment=Pull latest Raspberry executor branch and restart services only
Exec=lxterminal -e "bash -lc 'set -e; cd $PROJECT_DIR; echo Updating SignalMaker Raspberry executor...; git fetch origin; git checkout $BRANCH; git pull origin $BRANCH; chmod +x tui.sh scripts/*.sh 2>/dev/null || true; echo Restarting executor service...; sudo systemctl restart raspberry-executor.service || sudo systemctl restart raspberry-executor; echo Restarting TUI service...; sudo systemctl restart signalmaker-tui.service || true; echo; echo Update and restart complete. Press Enter to close.; read'"
Icon=system-software-update
Terminal=false
Categories=Utility;
EOF

cat > "$TUI_LAUNCHER_PATH" <<EOF
[Desktop Entry]
Type=Application
Name=SignalMaker Open TUI
Comment=Open the SignalMaker Raspberry terminal TUI
Exec=lxterminal -e "bash -lc 'set -e; cd $PROJECT_DIR; if [ -d .venv ]; then . .venv/bin/activate; fi; python -m raspberry_executor.tui_dashboard; echo; echo TUI closed. Press Enter to close.; read'"
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

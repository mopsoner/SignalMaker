#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

chmod +x tui.sh 2>/dev/null || true
chmod +x run_bot_service.sh 2>/dev/null || true
chmod +x scripts/install_system_start.sh 2>/dev/null || true

bash scripts/install_system_start.sh
bash go.sh

#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ARCH="$(uname -m 2>/dev/null || echo unknown)"

if [ "$ARCH" = "armv6l" ]; then
  echo "WARNING: ARMv6 detected: Vite/esbuild may crash with Bus error. Prefer prebuilt frontend/dist." >&2
fi

cd "$APP_DIR/frontend"
npm install
npm run build

#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
FRONTEND_DIR="$APP_DIR/frontend"
DIST_DIR="$FRONTEND_DIR/dist"

rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"
cp "$FRONTEND_DIR"/*.html "$FRONTEND_DIR/styles.css" "$FRONTEND_DIR/app.js" "$DIST_DIR/"
echo "Static frontend copied to frontend/dist (HTML/CSS/JS only; no Vite/esbuild)."

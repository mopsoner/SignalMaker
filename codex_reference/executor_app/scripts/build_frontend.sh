#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
FRONTEND_DIR="$APP_DIR/frontend"
DIST_DIR="$FRONTEND_DIR/dist"

rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"
# Keep the Raspberry frontend lightweight: copy the checked-in static assets only.
# This ensures frontend/dist/app.js always matches frontend/app.js without npm,
# Vite, esbuild, or any generated JavaScript bundle.
find "$FRONTEND_DIR" -path "$DIST_DIR" -prune -o -path "$FRONTEND_DIR/node_modules" -prune -o \
  -type f \( -name '*.html' -o -name '*.css' -o -name 'app.js' \) -print | while IFS= read -r asset; do
  relative_path="${asset#"$FRONTEND_DIR/"}"
  mkdir -p "$DIST_DIR/$(dirname "$relative_path")"
  cp "$asset" "$DIST_DIR/$relative_path"
done
echo "Static frontend copied to frontend/dist (HTML/CSS/JS only; no Vite/esbuild)."

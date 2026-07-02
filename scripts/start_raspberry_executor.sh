#!/usr/bin/env bash
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"

echo "DEPRECATED: scripts/start_raspberry_executor.sh is a compatibility wrapper." >&2
echo "Official Raspberry Executor entrypoint: ./run.sh device" >&2

REQ_FILE="requirements-raspberry.txt"
DEPS_MARKER=".deps_ok"
DEPS_HASH_FILE=".deps_ok.sha256"

requirements_hash() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$REQ_FILE" | awk '{print $1}'
  else
    python3 - <<'PY'
from hashlib import sha256
from pathlib import Path
print(sha256(Path('requirements-raspberry.txt').read_bytes()).hexdigest())
PY
  fi
}

if [ -x .venv/bin/python ]; then
  echo "Python venv already present; skipping venv creation."
else
  echo "Creating Python venv..."
  python3 -m venv .venv
fi

. .venv/bin/activate

current_hash="$(requirements_hash)"
previous_hash=""
if [ -f "$DEPS_HASH_FILE" ]; then
  previous_hash="$(cat "$DEPS_HASH_FILE" 2>/dev/null || true)"
fi

if [ -f "$DEPS_MARKER" ] && [ "$previous_hash" = "$current_hash" ]; then
  echo "Python dependencies already installed for current $REQ_FILE; skipping pip install."
else
  echo "Installing/updating Python dependencies because requirements changed or marker is missing..."
  python -m pip install --upgrade pip setuptools wheel
  pip install -r "$REQ_FILE"
  printf '%s\n' "$current_hash" > "$DEPS_HASH_FILE"
  touch "$DEPS_MARKER"
fi

if [ ! -f .env ]; then
  cp .env.example .env
fi

python -m raspberry_executor.install_sqlite
exec ./run.sh device "$@"

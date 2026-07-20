#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"

REQUIRED_PACKAGES=(
  git
  cron
  python3
  python3-venv
  python3-pip
  postgresql
  postgresql-contrib
  postgresql-client
  libpq-dev
  python3-dev
  build-essential
)

REQ_FILE="requirements-raspberry.txt"
DEPS_MARKER=".deps_ok"
DEPS_HASH_FILE=".deps_ok.sha256"

apt_install() {
  sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y "$@"
}

package_installed() {
  dpkg -s "$1" >/dev/null 2>&1
}

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

if ! command -v sudo >/dev/null 2>&1; then
  echo "sudo is required to install SignalMaker on Raspberry Pi" >&2
  exit 1
fi

if [ ! -f "$REQ_FILE" ]; then
  echo "$REQ_FILE is missing" >&2
  exit 1
fi

echo "Checking Raspberry Pi system dependencies..."
MISSING_PACKAGES=()
for package in "${REQUIRED_PACKAGES[@]}"; do
  if ! package_installed "$package"; then
    MISSING_PACKAGES+=("$package")
  fi
done

if ! package_installed postgresql-common; then
  MISSING_PACKAGES=(postgresql-common "${MISSING_PACKAGES[@]}")
fi

if [ "${#MISSING_PACKAGES[@]}" -gt 0 ]; then
  echo "Installing missing system packages: ${MISSING_PACKAGES[*]}"
  sudo apt-get update
  sudo dpkg --configure -a
  apt_install --fix-broken
  apt_install "${MISSING_PACKAGES[@]}"
else
  echo "System packages already present; skipping apt-get update/install."
fi

if ! command -v pg_lsclusters >/dev/null 2>&1; then
  echo "pg_lsclusters not available; repairing PostgreSQL common package..." >&2
  sudo apt-get update
  sudo dpkg --configure -a
  apt_install --reinstall postgresql-common
fi

echo "Enabling and starting PostgreSQL..."
sudo systemctl enable --now postgresql
if ! sudo systemctl is-active --quiet postgresql; then
  echo "PostgreSQL systemd service did not become active; database initialization cannot continue." >&2
  sudo systemctl status postgresql --no-pager >&2 || true
  exit 1
fi
if ! pg_isready -h localhost -p 5432; then
  echo "PostgreSQL is active but is not accepting connections on localhost:5432; database initialization cannot continue." >&2
  exit 1
fi

echo "Configuring PostgreSQL role and database..."
sudo -u postgres psql -v ON_ERROR_STOP=1 <<'SQL'
ALTER USER postgres WITH PASSWORD 'postgres';
SELECT 'CREATE DATABASE signalmaker OWNER postgres'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'signalmaker')\gexec
SQL

if [ ! -f ".env" ]; then
  if [ ! -f ".env.example" ]; then
    echo ".env.example is missing; cannot create .env" >&2
    exit 1
  fi
  cp .env.example .env
fi

env_value() {
  local key="$1"
  local default="$2"
  local value=""
  if [ -f ".env" ]; then
    value="$(awk -F= -v key="$key" '$1 == key {print $2; exit}' .env)"
  fi
  printf '%s' "${value:-$default}"
}

set_env_value() {
  local key="$1"
  local value="$2"
  if grep -q "^${key}=" .env; then
    sed -i "s|^${key}=.*|${key}=${value}|" .env
  else
    printf '\n%s=%s\n' "$key" "$value" >> .env
  fi
}

RASPBERRY_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
RASPBERRY_IP="${RASPBERRY_IP:-<raspberry-ip>}"
RASPBERRY_APP_PORT="${RASPBERRY_APP_PORT:-$(env_value APP_PORT 8080)}"
RASPBERRY_QUOTE_ASSETS="${RASPBERRY_QUOTE_ASSETS:-$(env_value QUOTE_ASSETS 'USD')}"
RASPBERRY_CORS_ORIGINS="http://localhost:${RASPBERRY_APP_PORT},http://127.0.0.1:${RASPBERRY_APP_PORT},http://${RASPBERRY_IP}:${RASPBERRY_APP_PORT}"
RASPBERRY_CORS_ORIGIN_REGEX=''

set_env_value APP_PORT "$RASPBERRY_APP_PORT"
set_env_value QUOTE_ASSETS "$RASPBERRY_QUOTE_ASSETS"

set_env_value CORS_ORIGINS "$RASPBERRY_CORS_ORIGINS"
if ! grep -q '^CORS_ORIGIN_REGEX=' .env; then
  printf 'CORS_ORIGIN_REGEX=%s\n' "$RASPBERRY_CORS_ORIGIN_REGEX" >> .env
fi

if [ -x .venv/bin/python ]; then
  echo "Python virtual environment already present; skipping venv creation."
else
  echo "Creating Python virtual environment..."
  python3 -m venv .venv --system-site-packages
fi

source .venv/bin/activate

CURRENT_REQ_HASH="$(requirements_hash)"
PREVIOUS_REQ_HASH=""
if [ -f "$DEPS_HASH_FILE" ]; then
  PREVIOUS_REQ_HASH="$(cat "$DEPS_HASH_FILE" 2>/dev/null || true)"
fi

if [ -f "$DEPS_MARKER" ] && [ "$PREVIOUS_REQ_HASH" = "$CURRENT_REQ_HASH" ]; then
  echo "Python dependencies already installed for current $REQ_FILE; skipping pip install."
else
  echo "Installing Python dependencies because requirements changed or marker is missing..."
  python -m pip install --upgrade pip setuptools wheel
  python -m pip install -r "$REQ_FILE"
  printf '%s\n' "$CURRENT_REQ_HASH" > "$DEPS_HASH_FILE"
  touch "$DEPS_MARKER"
fi

ARCH="$(uname -m 2>/dev/null || echo unknown)"
if [ "$ARCH" = "armv6l" ]; then
  echo "WARNING: ARMv6 detected. Using the lightweight static frontend; no Vite/esbuild build will run." >&2
fi

echo "Building lightweight frontend (HTML/CSS/JS only; no npm, Vite or esbuild)."
bash "$APP_DIR/scripts/build_frontend.sh"

mkdir -p logs data

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://postgres:postgres@localhost:5432/signalmaker}"

echo "Running database initialization..."
python -m scripts.init_db

echo "Initializing Raspberry executor SQLite database..."
python -m raspberry_executor.install_sqlite

install_run_sh_service() {
  echo "Installing raspberry-executor.service as the official startup mechanism..."

  # Remove legacy @reboot entries so cron and systemd can never launch in parallel.
  local tmp_cron
  if command -v crontab >/dev/null 2>&1; then
    tmp_cron="$(mktemp)"
    crontab -l 2>/dev/null \
      | grep -vF "cd $APP_DIR && /bin/bash run.sh" \
      | grep -vF "cd $APP_DIR && bash run.sh" \
      > "$tmp_cron" || true
    crontab "$tmp_cron"
    rm -f "$tmp_cron"
  fi

  sudo install -m 0644 "$APP_DIR/systemd/raspberry-executor.service" /etc/systemd/system/raspberry-executor.service
  sudo systemctl daemon-reload
  sudo systemctl enable --now raspberry-executor.service
}

install_run_sh_service

echo "Running final Raspberry install checks..."
pg_isready -h localhost -p 5432
python -m scripts.init_db

echo "After future frontend updates, rebuild and restart the single run.sh launcher:"
echo "  bash scripts/build_frontend.sh"
echo "  pkill -f 'run.sh' || true"
echo "  bash run.sh"
echo "Startup is managed by systemd: raspberry-executor.service"
echo "Startup logs: sudo journalctl -u raspberry-executor.service"
echo "Raspberry executor local SQLite database initialized at: ${APP_DIR}/raspberry_executor.db"

echo "SignalMaker Raspberry install complete"
echo "Run SignalMaker with:"
echo "  bash run.sh"
echo "SignalMaker UI/API: http://${RASPBERRY_IP}:${RASPBERRY_APP_PORT}"
echo "Admin UI: http://${RASPBERRY_IP}:${RASPBERRY_APP_PORT}/admin.html"

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

apt_install() {
  sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y "$@"
}

if ! command -v sudo >/dev/null 2>&1; then
  echo "sudo is required to install SignalMaker on Raspberry Pi" >&2
  exit 1
fi

if [ ! -f "requirements-raspberry.txt" ]; then
  echo "requirements-raspberry.txt is missing" >&2
  exit 1
fi

echo "Installing Raspberry Pi system dependencies..."
sudo apt-get update
apt_install --fix-broken
apt_install postgresql-common
if ! command -v pg_lsclusters >/dev/null 2>&1; then
  echo "pg_lsclusters was not installed by postgresql-common; repairing PostgreSQL packages..." >&2
  sudo dpkg --configure -a
  apt_install --reinstall postgresql-common
fi
apt_install "${REQUIRED_PACKAGES[@]}"
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y "${REQUIRED_PACKAGES[@]}"

echo "Enabling and starting PostgreSQL..."
sudo systemctl enable postgresql
sudo systemctl start postgresql

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
RASPBERRY_QUOTE_ASSETS="${RASPBERRY_QUOTE_ASSETS:-$(env_value QUOTE_ASSETS 'USD,USDC')}"
RASPBERRY_CORS_ORIGINS="http://localhost:${RASPBERRY_APP_PORT},http://127.0.0.1:${RASPBERRY_APP_PORT},http://${RASPBERRY_IP}:${RASPBERRY_APP_PORT}"
RASPBERRY_CORS_ORIGIN_REGEX=''

set_env_value APP_PORT "$RASPBERRY_APP_PORT"
set_env_value QUOTE_ASSETS "$RASPBERRY_QUOTE_ASSETS"

set_env_value CORS_ORIGINS "$RASPBERRY_CORS_ORIGINS"
if ! grep -q '^CORS_ORIGIN_REGEX=' .env; then
  printf 'CORS_ORIGIN_REGEX=%s\n' "$RASPBERRY_CORS_ORIGIN_REGEX" >> .env
fi

echo "Creating a fresh Python virtual environment..."
rm -rf .venv
python3 -m venv .venv --system-site-packages
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements-raspberry.txt

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

install_run_sh_startup() {
  if ! command -v crontab >/dev/null 2>&1; then
    echo "crontab not found; skipping run.sh startup registration" >&2
    return 0
  fi

  echo "Registering run.sh to start SignalMaker at Raspberry boot..."
  local startup_line
  startup_line="@reboot cd $APP_DIR && /bin/bash run.sh >> $APP_DIR/logs/startup.log 2>&1"
  local tmp_cron
  tmp_cron="$(mktemp)"

  crontab -l 2>/dev/null \
    | grep -vF "cd $APP_DIR && /bin/bash run.sh" \
    | grep -vF "cd $APP_DIR && bash run.sh" \
    > "$tmp_cron" || true
  printf '%s\n' "$startup_line" >> "$tmp_cron"
  crontab "$tmp_cron"
  rm -f "$tmp_cron"
}

install_run_sh_startup

echo "Running final Raspberry install checks..."
pg_isready -h localhost -p 5432
python -m scripts.init_db

echo "After future frontend updates, rebuild and restart the single run.sh launcher:"
echo "  bash scripts/build_frontend.sh"
echo "  pkill -f 'run.sh' || true"
echo "  bash run.sh"
echo "Startup is registered in the current user crontab with: @reboot cd ${APP_DIR} && /bin/bash run.sh"
echo "Startup logs: ${APP_DIR}/logs/startup.log"

echo "SignalMaker Raspberry install complete"
echo "Run SignalMaker with:"
echo "  bash run.sh"
echo "SignalMaker UI/API: http://${RASPBERRY_IP}:${RASPBERRY_APP_PORT}"
echo "Admin UI: http://${RASPBERRY_IP}:${RASPBERRY_APP_PORT}/admin.html"

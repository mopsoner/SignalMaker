#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"

REQUIRED_PACKAGES=(
  git
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

echo "Creating a fresh Python virtual environment..."
rm -rf .venv
python3 -m venv .venv --system-site-packages
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements-raspberry.txt

mkdir -p logs data

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://postgres:postgres@localhost:5432/signalmaker}"

echo "Running database initialization..."
python -m scripts.init_db

echo "Running final Raspberry install checks..."
pg_isready -h localhost -p 5432
python -m scripts.init_db

echo "SignalMaker Raspberry install complete"
echo "Run SignalMaker with:"
echo "  bash run.sh api"
echo "  bash run.sh executor-loop"
echo "  bash run.sh pipeline-loop"
echo "  bash run.sh scheduler-loop"

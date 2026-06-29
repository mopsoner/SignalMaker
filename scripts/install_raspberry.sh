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

echo "Creating a fresh Python virtual environment..."
rm -rf .venv
python3 -m venv .venv --system-site-packages
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements-raspberry.txt

ARCH="$(uname -m 2>/dev/null || echo unknown)"
if [ "$ARCH" = "armv6l" ]; then
  echo "WARNING: ARMv6 detected. Build the frontend elsewhere and copy frontend/dist to this Raspberry Pi." >&2
fi

if [ ! -d "$APP_DIR/frontend/dist" ]; then
  echo "WARNING: frontend/dist is missing. The frontend service will stay stopped until dist is copied." >&2
  echo "WARNING: ARMv6 detected: Vite/esbuild may crash with Bus error. Prefer prebuilt frontend/dist." >&2
fi

echo "Installing frontend dependencies and attempting a production build..."
echo "WARNING: Raspberry Pi Node/Vite/esbuild builds can fail with Bus error on older ARM devices." >&2
echo "WARNING: Installation will continue if npm install or npm run build fails." >&2
cd "$APP_DIR/frontend"
if ! npm install; then
  echo "WARNING: npm install failed. Continuing installation; copy a prebuilt frontend/dist or run scripts/build_frontend.sh on a compatible machine." >&2
elif ! npm run build; then
  echo "WARNING: npm run build failed. Continuing installation; copy a prebuilt frontend/dist from a compatible machine." >&2
fi
if [ ! -d "$APP_DIR/frontend/dist" ]; then
  echo "WARNING: frontend/dist is not available. The frontend service will fail cleanly until dist is copied or FRONTEND_DEV_SERVER=true is set manually." >&2
fi

mkdir -p logs data

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://postgres:postgres@localhost:5432/signalmaker}"

echo "Running database initialization..."
python -m scripts.init_db

install_systemd_services() {
  if ! command -v systemctl >/dev/null 2>&1; then
    echo "systemd not found; skipping SignalMaker service installation" >&2
    return 0
  fi

  echo "Installing SignalMaker systemd services for $APP_DIR..."
  local service_dir
  service_dir="$(mktemp -d)"

  cat > "$service_dir/signalmaker-api.service" <<EOF
[Unit]
Description=SignalMaker API
After=network.target postgresql.service

[Service]
Type=simple
WorkingDirectory=$APP_DIR
ExecStart=/bin/bash $APP_DIR/scripts/start_api.sh
Restart=always
RestartSec=5
Environment=APP_PORT=8080

[Install]
WantedBy=multi-user.target
EOF

  cat > "$service_dir/signalmaker-executor.service" <<EOF
[Unit]
Description=SignalMaker Executor Worker
After=network.target postgresql.service signalmaker-api.service

[Service]
Type=simple
WorkingDirectory=$APP_DIR
ExecStart=/bin/bash $APP_DIR/scripts/start_executor_worker.sh
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

  cat > "$service_dir/signalmaker-pipeline.service" <<EOF
[Unit]
Description=SignalMaker Pipeline Worker
After=network.target postgresql.service signalmaker-api.service

[Service]
Type=simple
WorkingDirectory=$APP_DIR
ExecStart=/bin/bash $APP_DIR/scripts/start_pipeline_worker.sh
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

  cat > "$service_dir/signalmaker-scheduler.service" <<EOF
[Unit]
Description=SignalMaker Scheduler Worker
After=network.target postgresql.service signalmaker-api.service

[Service]
Type=simple
WorkingDirectory=$APP_DIR
ExecStart=/bin/bash $APP_DIR/scripts/start_scheduler_worker.sh
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

  cat > "$service_dir/signalmaker-frontend.service" <<EOF
[Unit]
Description=SignalMaker Frontend UI
After=network.target signalmaker-api.service

[Service]
Type=simple
WorkingDirectory=$APP_DIR
ExecStart=/bin/bash $APP_DIR/scripts/start_frontend.sh
Restart=on-failure
RestartSec=5
Environment=FRONTEND_PORT=3000
Environment=VITE_API_BASE=http://127.0.0.1:8080

[Install]
WantedBy=multi-user.target
EOF

  sudo cp "$service_dir"/signalmaker-*.service /etc/systemd/system/
  sudo chmod 644 /etc/systemd/system/signalmaker-*.service
  sudo systemctl daemon-reload
  sudo systemctl enable signalmaker-api signalmaker-executor signalmaker-pipeline signalmaker-scheduler signalmaker-frontend
  rm -rf "$service_dir"
}

install_systemd_services

echo "Running final Raspberry install checks..."
pg_isready -h localhost -p 5432
command -v npm
python -m scripts.init_db

RASPBERRY_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
RASPBERRY_IP="${RASPBERRY_IP:-<raspberry-ip>}"

echo "SignalMaker Raspberry install complete"
echo "Run SignalMaker with:"
echo "  bash run.sh api"
echo "  bash run.sh executor-loop"
echo "  bash run.sh pipeline-loop"
echo "  bash run.sh scheduler-loop"
echo "  bash scripts/start_frontend.sh"
echo "Backend API: http://${RASPBERRY_IP}:8080"
echo "Frontend UI: http://${RASPBERRY_IP}:3000"

#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

APP_DIR="$(pwd)"
DATABASE_NAME="${DATABASE_NAME:-signalmaker}"
DATABASE_USER="${DATABASE_USER:-postgres}"
SERVICES=(
  signalmaker-frontend.service
  signalmaker-scheduler.service
  signalmaker-pipeline.service
  signalmaker-executor.service
  signalmaker-api.service
)
GENERATED_PATHS=(
  "$APP_DIR/.env"
  "$APP_DIR/logs"
  "$APP_DIR/data"
)
SQLITE_DATABASE_PATHS=(
  "$APP_DIR/raspberry_executor.db"
  "$APP_DIR/raspberry_executor.db-wal"
  "$APP_DIR/raspberry_executor.db-shm"
)

run_root() {
  if [ "$(id -u)" -eq 0 ]; then
    "$@"
  else
    sudo "$@"
  fi
}

remove_systemd_services() {
  if ! command -v systemctl >/dev/null 2>&1; then
    echo "systemd not found; skipping service removal"
    return 0
  fi

  for service in "${SERVICES[@]}"; do
    run_root systemctl stop "$service" >/dev/null 2>&1 || true
    run_root systemctl disable "$service" >/dev/null 2>&1 || true
    run_root rm -f "/etc/systemd/system/$service"
  done

  run_root systemctl daemon-reload
  run_root systemctl reset-failed >/dev/null 2>&1 || true
}

remove_generated_files() {
  for path in "${GENERATED_PATHS[@]}"; do
    if [ -e "$path" ]; then
      rm -rf "$path"
      echo "Removed: $path"
    fi
  done
}

remove_sqlite_database() {
  for path in "${SQLITE_DATABASE_PATHS[@]}"; do
    if [ -e "$path" ]; then
      rm -f "$path"
      echo "Removed SQLite database file: $path"
    fi
  done
}

remove_database() {
  if ! command -v psql >/dev/null 2>&1; then
    echo "psql not found; skipping PostgreSQL database removal"
    return 0
  fi

  if ! command -v sudo >/dev/null 2>&1; then
    echo "sudo not found; skipping PostgreSQL database removal"
    return 0
  fi

  if ! sudo -u "$DATABASE_USER" psql -d postgres -tAc "SELECT 1" >/dev/null 2>&1; then
    echo "Cannot connect to PostgreSQL as $DATABASE_USER; skipping database removal"
    return 0
  fi

  sudo -u "$DATABASE_USER" psql -d postgres -v ON_ERROR_STOP=1 <<SQL
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '${DATABASE_NAME}'
  AND pid <> pg_backend_pid();
DROP DATABASE IF EXISTS ${DATABASE_NAME};
SQL
}

echo "Uninstalling SignalMaker Raspberry services from $APP_DIR..."
remove_systemd_services

echo "Removing SignalMaker generated files..."
remove_generated_files

echo "Removing SignalMaker executor SQLite database..."
remove_sqlite_database

echo "Removing SignalMaker PostgreSQL database..."
remove_database

cat <<EOF_SUMMARY
SignalMaker Raspberry uninstall complete.
Removed systemd units: ${SERVICES[*]}
Removed generated files: ${GENERATED_PATHS[*]}
Removed executor SQLite database files: ${SQLITE_DATABASE_PATHS[*]}
Removed PostgreSQL database: ${DATABASE_NAME} when reachable
Kept apt packages installed by scripts/install_raspberry.sh.
Kept pip packages and the Python virtual environment (.venv) in place.
EOF_SUMMARY

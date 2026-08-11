#!/usr/bin/env bash
set -u

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BRANCH="${SIGNALMAKER_BRANCH:-${BRANCH:-raspberry/executor-app}}"
REMOTE="${SIGNALMAKER_REMOTE:-origin}"
LOCK_FILE="${APP_DIR}/.self_update.lock"
LOG_PREFIX="[self-update]"

cd "$APP_DIR"

if [ ! -d .git ]; then
  echo "$LOG_PREFIX no git repo at $APP_DIR, skipping"
  exit 0
fi

# Prevent the bot and TUI from updating the same checkout at the same time.
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "$LOG_PREFIX another update is running, skipping"
  exit 0
fi

current_branch="$(git branch --show-current 2>/dev/null || true)"
current_sha="$(git rev-parse --short HEAD 2>/dev/null || true)"

echo "$LOG_PREFIX checking $REMOTE/$BRANCH from $current_branch@$current_sha"

git fetch "$REMOTE" "$BRANCH" || {
  echo "$LOG_PREFIX fetch failed, keeping current version"
  exit 0
}

# Keep runtime/private files and generated state, but remove local code changes
# that would block updates. This avoids conflicts on root launcher scripts.
git checkout "$BRANCH" 2>/dev/null || git checkout -B "$BRANCH" "$REMOTE/$BRANCH" || {
  echo "$LOG_PREFIX checkout failed, keeping current version"
  exit 0
}

git reset --hard "$REMOTE/$BRANCH" || {
  echo "$LOG_PREFIX reset failed, keeping current version"
  exit 0
}

# Clean generated/untracked files except runtime config, venv, DB/state and logs.
git clean -fd \
  -e .env \
  -e .venv/ \
  -e .deps_ok \
  -e data/ \
  -e logs/ \
  -e '*.db' \
  -e '*.sqlite' \
  -e '*.sqlite3' || true

chmod +x install_raspberry_executor.sh run.sh tui.sh scripts/*.sh 2>/dev/null || true

new_sha="$(git rev-parse --short HEAD 2>/dev/null || true)"
if [ "$current_sha" != "$new_sha" ]; then
  echo "$LOG_PREFIX updated $current_sha -> $new_sha"
else
  echo "$LOG_PREFIX already up to date at $new_sha"
fi

exit 0

#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

exec bash scripts/start_reserved_vm.sh "$@"

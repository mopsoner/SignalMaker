#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
exec python -m scripts.run_kraken_profit_loop

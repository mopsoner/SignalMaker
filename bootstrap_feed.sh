#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$APP_DIR"

mkdir -p logs

if [ ! -f ".venv/bin/activate" ]; then
  echo "❌ .venv introuvable. Lance d'abord : bash install_raspberry_executor.sh"
  exit 1
fi

if [ ! -f "scripts/bootstrap_wyckoff_candles.py" ]; then
  echo "❌ scripts/bootstrap_wyckoff_candles.py introuvable."
  echo "Le script bootstrap Python doit exister avant de lancer bootstrap_feed.sh"
  exit 1
fi

# Quote asset:
# - par défaut: USD
# - override possible: ./bootstrap_feed.sh USDC
BOOTSTRAP_QUOTES="${1:-${BOOTSTRAP_QUOTES:-USD}}"
BOOTSTRAP_QUOTES="$(echo "$BOOTSTRAP_QUOTES" | tr '[:lower:]' '[:upper:]')"

# Bootstrap defaults
BOOTSTRAP_MAX_SYMBOLS="${BOOTSTRAP_MAX_SYMBOLS:-300}"
BOOTSTRAP_MARGIN_ONLY="${BOOTSTRAP_MARGIN_ONLY:-true}"
BOOTSTRAP_INTERVALS="${BOOTSTRAP_INTERVALS:-15m,1h,4h}"
BOOTSTRAP_MIN_15M="${BOOTSTRAP_MIN_15M:-180}"
BOOTSTRAP_MIN_1H="${BOOTSTRAP_MIN_1H:-180}"
BOOTSTRAP_MIN_4H="${BOOTSTRAP_MIN_4H:-120}"
BOOTSTRAP_KRAKEN_RPM="${BOOTSTRAP_KRAKEN_RPM:-60}"
BOOTSTRAP_POST_CHUNK_SIZE="${BOOTSTRAP_POST_CHUNK_SIZE:-60}"

echo "=== Bootstrap feed Wyckoff candles ==="
echo "App dir: $APP_DIR"
echo "Quotes: $BOOTSTRAP_QUOTES"
echo "Margin only: $BOOTSTRAP_MARGIN_ONLY"
echo "Max symbols: $BOOTSTRAP_MAX_SYMBOLS"
echo "Intervals: $BOOTSTRAP_INTERVALS"
echo "Min 15m: $BOOTSTRAP_MIN_15M"
echo "Min 1h: $BOOTSTRAP_MIN_1H"
echo "Min 4h: $BOOTSTRAP_MIN_4H"
echo "Kraken RPM: $BOOTSTRAP_KRAKEN_RPM"
echo "Post chunk size: $BOOTSTRAP_POST_CHUNK_SIZE"
echo "Log file: $APP_DIR/logs/bootstrap_wyckoff_candles.log"
echo "======================================"

source .venv/bin/activate

PYTHONPATH="$APP_DIR" \
BOOTSTRAP_QUOTES="$BOOTSTRAP_QUOTES" \
BOOTSTRAP_MAX_SYMBOLS="$BOOTSTRAP_MAX_SYMBOLS" \
BOOTSTRAP_MARGIN_ONLY="$BOOTSTRAP_MARGIN_ONLY" \
BOOTSTRAP_INTERVALS="$BOOTSTRAP_INTERVALS" \
BOOTSTRAP_MIN_15M="$BOOTSTRAP_MIN_15M" \
BOOTSTRAP_MIN_1H="$BOOTSTRAP_MIN_1H" \
BOOTSTRAP_MIN_4H="$BOOTSTRAP_MIN_4H" \
BOOTSTRAP_KRAKEN_RPM="$BOOTSTRAP_KRAKEN_RPM" \
BOOTSTRAP_POST_CHUNK_SIZE="$BOOTSTRAP_POST_CHUNK_SIZE" \
python scripts/bootstrap_wyckoff_candles.py 2>&1 | tee -a logs/bootstrap_wyckoff_candles.log

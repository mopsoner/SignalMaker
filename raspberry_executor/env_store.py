from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
EXAMPLE_PATH = ROOT / ".env.raspberry.example"

DEFAULTS = {
    "SIGNALMAKER_BASE_URL": "https://your-signalmaker-app.replit.app",
    "GATEWAY_ID": "raspberry-fr-1",
    "POLL_SECONDS": "15",
    "DRY_RUN": "true",
    "QUOTE_ASSETS": "USDT",
    "ALLOW_SHORTS": "false",
    "ORDER_QUOTE_AMOUNT": "20",
    "MAX_CANDIDATE_AGE_SECONDS": "900",
    "BINANCE_BASE_URL": "https://api.binance.com",
    "BINANCE_API_KEY": "",
    "BINANCE_SECRET_KEY": "",
    "CANDLE_FEED_ENABLED": "true",
    "CANDLE_FEED_INTERVALS": "15m,1h,4h",
    "CANDLE_FEED_LIMIT": "50",
    "CANDLE_FEED_POLL_SECONDS": "180",
    "CANDLE_FEED_MAX_SYMBOLS": "10",
    "CANDLE_FEED_MAX_WORKERS": "3",
    "CANDLE_FEED_BINANCE_REQUESTS_PER_MINUTE": "300",
    "CANDLE_FEED_SMOKE_SYMBOL_LIMIT": "3",
    "WEB_HOST": "0.0.0.0",
    "WEB_PORT": "8090",
    "ADMIN_PASSWORD": "",
}

LEGACY_KEYS = {
    "EXECUTION_QUOTE_ASSET",
    "ALLOWED_SYMBOLS",
    "CANDLE_FEED_QUOTES",
    "CANDLE_FEED_QUOTE_ASSETS",
    "CANDLE_FEED_SYMBOLS",
}

SECRET_KEYS = {"BINANCE_API_KEY", "BINANCE_SECRET_KEY", "ADMIN_PASSWORD"}


def _normalize_quotes(value: str | None) -> str:
    return ",".join(item.strip().upper() for item in (value or "").split(",") if item.strip())


def ensure_env() -> None:
    if ENV_PATH.exists():
        return
    if EXAMPLE_PATH.exists():
        ENV_PATH.write_text(EXAMPLE_PATH.read_text())
    else:
        write_env(DEFAULTS)


def read_env() -> dict[str, str]:
    ensure_env()
    values = DEFAULTS.copy()
    legacy_values: dict[str, str] = {}
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key in DEFAULTS:
            values[key] = value
        elif key in LEGACY_KEYS:
            legacy_values[key] = value

    if not values.get("QUOTE_ASSETS"):
        values["QUOTE_ASSETS"] = (
            legacy_values.get("CANDLE_FEED_QUOTES")
            or legacy_values.get("CANDLE_FEED_QUOTE_ASSETS")
            or legacy_values.get("ALLOWED_SYMBOLS")
            or legacy_values.get("EXECUTION_QUOTE_ASSET")
            or DEFAULTS["QUOTE_ASSETS"]
        )
    values["QUOTE_ASSETS"] = _normalize_quotes(values.get("QUOTE_ASSETS")) or DEFAULTS["QUOTE_ASSETS"]
    return values


def write_env(values: dict[str, str]) -> None:
    merged = DEFAULTS.copy()
    merged.update({key: str(value) for key, value in values.items() if key in DEFAULTS})
    merged["QUOTE_ASSETS"] = _normalize_quotes(merged.get("QUOTE_ASSETS")) or DEFAULTS["QUOTE_ASSETS"]
    lines = [
        f"SIGNALMAKER_BASE_URL={merged['SIGNALMAKER_BASE_URL']}",
        f"GATEWAY_ID={merged['GATEWAY_ID']}",
        f"POLL_SECONDS={merged['POLL_SECONDS']}",
        f"DRY_RUN={merged['DRY_RUN']}",
        f"QUOTE_ASSETS={merged['QUOTE_ASSETS']}",
        f"ALLOW_SHORTS={merged['ALLOW_SHORTS']}",
        f"ORDER_QUOTE_AMOUNT={merged['ORDER_QUOTE_AMOUNT']}",
        f"MAX_CANDIDATE_AGE_SECONDS={merged['MAX_CANDIDATE_AGE_SECONDS']}",
        "",
        f"BINANCE_BASE_URL={merged['BINANCE_BASE_URL']}",
        f"BINANCE_API_KEY={merged['BINANCE_API_KEY']}",
        f"BINANCE_SECRET_KEY={merged['BINANCE_SECRET_KEY']}",
        "",
        f"CANDLE_FEED_ENABLED={merged['CANDLE_FEED_ENABLED']}",
        f"CANDLE_FEED_INTERVALS={merged['CANDLE_FEED_INTERVALS']}",
        f"CANDLE_FEED_LIMIT={merged['CANDLE_FEED_LIMIT']}",
        f"CANDLE_FEED_POLL_SECONDS={merged['CANDLE_FEED_POLL_SECONDS']}",
        f"CANDLE_FEED_MAX_SYMBOLS={merged['CANDLE_FEED_MAX_SYMBOLS']}",
        f"CANDLE_FEED_MAX_WORKERS={merged['CANDLE_FEED_MAX_WORKERS']}",
        f"CANDLE_FEED_BINANCE_REQUESTS_PER_MINUTE={merged['CANDLE_FEED_BINANCE_REQUESTS_PER_MINUTE']}",
        f"CANDLE_FEED_SMOKE_SYMBOL_LIMIT={merged['CANDLE_FEED_SMOKE_SYMBOL_LIMIT']}",
        "",
        f"WEB_HOST={merged['WEB_HOST']}",
        f"WEB_PORT={merged['WEB_PORT']}",
        f"ADMIN_PASSWORD={merged['ADMIN_PASSWORD']}",
        "",
    ]
    ENV_PATH.write_text("\n".join(lines))


def public_env() -> dict[str, str]:
    values = read_env()
    safe = values.copy()
    for key in SECRET_KEYS:
        if safe.get(key):
            safe[key] = "********"
    return safe

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
EXAMPLE_PATH = ROOT / ".env.raspberry.example"

DEFAULTS = {
    "SIGNALMAKER_BASE_URL": "https://your-signalmaker-app.replit.app",
    "GATEWAY_ID": "raspberry-fr-1",
    "POLL_SECONDS": "15",
    "DRY_RUN": "true",
    "EXECUTION_QUOTE_ASSET": "USDC",
    "ALLOWED_SYMBOLS": "",
    "ORDER_QUOTE_AMOUNT": "20",
    "MAX_CANDIDATE_AGE_SECONDS": "900",
    "BINANCE_BASE_URL": "https://api.binance.com",
    "BINANCE_API_KEY": "",
    "BINANCE_SECRET_KEY": "",
    "CANDLE_FEED_ENABLED": "true",
    "CANDLE_FEED_QUOTES": "USDT",
    "CANDLE_FEED_INTERVALS": "15m,1h,4h",
    "CANDLE_FEED_LIMIT": "50",
    "CANDLE_FEED_POLL_SECONDS": "180",
    "CANDLE_FEED_MAX_SYMBOLS": "10",
    "CANDLE_FEED_SMOKE_SYMBOL_LIMIT": "3",
    "CANDLE_FEED_SYMBOLS": "",
    "CANDLE_FEED_QUOTE_ASSETS": "",
    "WEB_HOST": "0.0.0.0",
    "WEB_PORT": "8090",
    "ADMIN_PASSWORD": "",
}

SECRET_KEYS = {"BINANCE_API_KEY", "BINANCE_SECRET_KEY", "ADMIN_PASSWORD"}


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
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    if not values.get("CANDLE_FEED_QUOTES") and values.get("CANDLE_FEED_QUOTE_ASSETS"):
        values["CANDLE_FEED_QUOTES"] = values["CANDLE_FEED_QUOTE_ASSETS"]
    return values


def write_env(values: dict[str, str]) -> None:
    merged = DEFAULTS.copy()
    merged.update({key: str(value) for key, value in values.items() if key in DEFAULTS})
    if not merged.get("CANDLE_FEED_QUOTES") and merged.get("CANDLE_FEED_QUOTE_ASSETS"):
        merged["CANDLE_FEED_QUOTES"] = merged["CANDLE_FEED_QUOTE_ASSETS"]
    merged["CANDLE_FEED_QUOTE_ASSETS"] = merged.get("CANDLE_FEED_QUOTES", "")
    lines = [
        f"SIGNALMAKER_BASE_URL={merged['SIGNALMAKER_BASE_URL']}",
        f"GATEWAY_ID={merged['GATEWAY_ID']}",
        f"POLL_SECONDS={merged['POLL_SECONDS']}",
        f"DRY_RUN={merged['DRY_RUN']}",
        "",
        f"EXECUTION_QUOTE_ASSET={merged['EXECUTION_QUOTE_ASSET']}",
        f"ALLOWED_SYMBOLS={merged['ALLOWED_SYMBOLS']}",
        f"ORDER_QUOTE_AMOUNT={merged['ORDER_QUOTE_AMOUNT']}",
        f"MAX_CANDIDATE_AGE_SECONDS={merged['MAX_CANDIDATE_AGE_SECONDS']}",
        "",
        f"BINANCE_BASE_URL={merged['BINANCE_BASE_URL']}",
        f"BINANCE_API_KEY={merged['BINANCE_API_KEY']}",
        f"BINANCE_SECRET_KEY={merged['BINANCE_SECRET_KEY']}",
        "",
        f"CANDLE_FEED_ENABLED={merged['CANDLE_FEED_ENABLED']}",
        f"CANDLE_FEED_QUOTES={merged['CANDLE_FEED_QUOTES']}",
        f"CANDLE_FEED_INTERVALS={merged['CANDLE_FEED_INTERVALS']}",
        f"CANDLE_FEED_LIMIT={merged['CANDLE_FEED_LIMIT']}",
        f"CANDLE_FEED_POLL_SECONDS={merged['CANDLE_FEED_POLL_SECONDS']}",
        f"CANDLE_FEED_MAX_SYMBOLS={merged['CANDLE_FEED_MAX_SYMBOLS']}",
        f"CANDLE_FEED_SMOKE_SYMBOL_LIMIT={merged['CANDLE_FEED_SMOKE_SYMBOL_LIMIT']}",
        f"CANDLE_FEED_SYMBOLS={merged['CANDLE_FEED_SYMBOLS']}",
        f"CANDLE_FEED_QUOTE_ASSETS={merged['CANDLE_FEED_QUOTE_ASSETS']}",
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

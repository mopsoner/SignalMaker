from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
EXAMPLE_PATH = ROOT / ".env.raspberry.example"

# Bootstrap defaults for the Raspberry process. Admin/runtime-managed values
# should be migrated to app_settings and read through the API/runtime settings;
# this file remains only as startup fallback and for Raspberry-local fields.
DEFAULTS = {
    "SIGNALMAKER_BASE_URL": "https://mysginalmaker.replit.app",
    "GATEWAY_ID": "raspberry-fr-1",
    "POLL_SECONDS": "15",
    "DRY_RUN": "false",
    "QUOTE_ASSETS": "USD,USDC",
    "ORDER_QUOTE_AMOUNT": "20",
    "MAX_CANDIDATE_AGE_SECONDS": "900",
    "EXECUTION_EXCHANGE": "kraken",
    "KRAKEN_BASE_URL": "https://api.kraken.com",
    "KRAKEN_API_KEY": "",
    "KRAKEN_SECRET_KEY": "",
    "CANDLE_FEED_ENABLED": "true",
    "CANDLE_FEED_INTERVALS": "15m,1h,4h",
    "CANDLE_FEED_LIMIT": "50",
    "CANDLE_FEED_POLL_SECONDS": "300",
    "CANDLE_FEED_MAX_SYMBOLS": "400",
    "CANDLE_FEED_MAX_WORKERS": "1",
    "CANDLE_FEED_KRAKEN_REQUESTS_PER_MINUTE": "300",
    "CANDLE_FEED_SMOKE_SYMBOL_LIMIT": "3",
    "MOMENTUM_DECISION_ENABLED": "true",
    "MOMENTUM_DECISION_EXECUTE_ENABLED": "true",
    "MOMENTUM_DECISION_POLL_SECONDS": "300",
    "MOMENTUM_DECISION_PATH": "/api/v1/momentum",
    "MOMENTUM_DECISION_METHOD": "GET",
    "MOMENTUM_DECISION_LIMIT": "25",
    "MOMENTUM_CANDIDATES_PATH": "/api/v1/momentum",
    "MOMENTUM_DECISION_CANDIDATES_FALLBACK_ENABLED": "true",
    "MOMENTUM_DECISION_FALLBACK_LIMIT": "50",
    "MOMENTUM_DECISION_CADENCE_HOURS": "4",
    "MOMENTUM_DECISION_STARTING_CAPITAL": "1000",
    "MOMENTUM_DECISION_MIN_SCORE": "0",
    "MOMENTUM_BUYABLE_RSI_1H_MIN": "45",
    "MOMENTUM_BUYABLE_RSI_1H_MAX": "55",
    "APP_PORT": "8080",
    "EXECUTOR_API_PORT": "8080",
    "EXECUTOR_DASHBOARD_PORT": "8080",
    "WEB_HOST": "0.0.0.0",
    "ADMIN_PASSWORD": "",
    "DATABASE_URL": "postgresql+psycopg://postgres:postgres@localhost:5432/signalmaker",
}

LEGACY_KEYS = {
    "EXECUTION_QUOTE_ASSET",
    "ALLOWED_SYMBOLS",
    "CANDLE_FEED_QUOTES",
    "CANDLE_FEED_QUOTE_ASSETS",
    "CANDLE_FEED_SYMBOLS",
    "ALLOW_SHORTS",
}

SECRET_KEYS = {"KRAKEN_API_KEY", "KRAKEN_SECRET_KEY", "ADMIN_PASSWORD"}


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
        f"ORDER_QUOTE_AMOUNT={merged['ORDER_QUOTE_AMOUNT']}",
        f"MAX_CANDIDATE_AGE_SECONDS={merged['MAX_CANDIDATE_AGE_SECONDS']}",
        "",
        f"EXECUTION_EXCHANGE={merged['EXECUTION_EXCHANGE']}",
        f"KRAKEN_BASE_URL={merged['KRAKEN_BASE_URL']}",
        f"KRAKEN_API_KEY={merged['KRAKEN_API_KEY']}",
        f"KRAKEN_SECRET_KEY={merged['KRAKEN_SECRET_KEY']}",
        "",
        f"CANDLE_FEED_ENABLED={merged['CANDLE_FEED_ENABLED']}",
        f"CANDLE_FEED_INTERVALS={merged['CANDLE_FEED_INTERVALS']}",
        f"CANDLE_FEED_LIMIT={merged['CANDLE_FEED_LIMIT']}",
        f"CANDLE_FEED_POLL_SECONDS={merged['CANDLE_FEED_POLL_SECONDS']}",
        f"CANDLE_FEED_MAX_SYMBOLS={merged['CANDLE_FEED_MAX_SYMBOLS']}",
        f"CANDLE_FEED_MAX_WORKERS={merged['CANDLE_FEED_MAX_WORKERS']}",
        f"CANDLE_FEED_KRAKEN_REQUESTS_PER_MINUTE={merged['CANDLE_FEED_KRAKEN_REQUESTS_PER_MINUTE']}",
        f"CANDLE_FEED_SMOKE_SYMBOL_LIMIT={merged['CANDLE_FEED_SMOKE_SYMBOL_LIMIT']}",
        "",
        f"MOMENTUM_DECISION_ENABLED={merged['MOMENTUM_DECISION_ENABLED']}",
        f"MOMENTUM_DECISION_EXECUTE_ENABLED={merged['MOMENTUM_DECISION_EXECUTE_ENABLED']}",
        f"MOMENTUM_DECISION_POLL_SECONDS={merged['MOMENTUM_DECISION_POLL_SECONDS']}",
        f"MOMENTUM_DECISION_PATH={merged['MOMENTUM_DECISION_PATH']}",
        f"MOMENTUM_DECISION_METHOD={merged['MOMENTUM_DECISION_METHOD']}",
        f"MOMENTUM_DECISION_LIMIT={merged['MOMENTUM_DECISION_LIMIT']}",
        f"MOMENTUM_CANDIDATES_PATH={merged['MOMENTUM_CANDIDATES_PATH']}",
        f"MOMENTUM_DECISION_CANDIDATES_FALLBACK_ENABLED={merged['MOMENTUM_DECISION_CANDIDATES_FALLBACK_ENABLED']}",
        f"MOMENTUM_DECISION_FALLBACK_LIMIT={merged['MOMENTUM_DECISION_FALLBACK_LIMIT']}",
        f"MOMENTUM_DECISION_CADENCE_HOURS={merged['MOMENTUM_DECISION_CADENCE_HOURS']}",
        f"MOMENTUM_DECISION_STARTING_CAPITAL={merged['MOMENTUM_DECISION_STARTING_CAPITAL']}",
        f"MOMENTUM_DECISION_MIN_SCORE={merged['MOMENTUM_DECISION_MIN_SCORE']}",
        f"MOMENTUM_BUYABLE_RSI_1H_MIN={merged['MOMENTUM_BUYABLE_RSI_1H_MIN']}",
        f"MOMENTUM_BUYABLE_RSI_1H_MAX={merged['MOMENTUM_BUYABLE_RSI_1H_MAX']}",
        "",
        f"APP_PORT={merged['APP_PORT']}",
        f"EXECUTOR_API_PORT={merged['EXECUTOR_API_PORT']}",
        f"EXECUTOR_DASHBOARD_PORT={merged['EXECUTOR_DASHBOARD_PORT']}",
        f"WEB_HOST={merged['WEB_HOST']}",
        f"ADMIN_PASSWORD={merged['ADMIN_PASSWORD']}",
        "",
        f"DATABASE_URL={merged['DATABASE_URL']}",
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

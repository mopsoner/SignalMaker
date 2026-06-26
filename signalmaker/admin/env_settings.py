import os
from datetime import datetime
from signalmaker.data_providers.eodhd.config import get_eodhd_config

ENV_VARS = ["EODHD_ENABLED","EODHD_API_KEY","EODHD_BASE_URL","EODHD_DEFAULT_EXCHANGE","EODHD_DEFAULT_TIMEFRAME","EODHD_REQUEST_SLEEP_SECONDS","EODHD_MAX_CONCURRENT","EODHD_ADJUSTED_DATA","EODHD_START_DATE","MARKET_DATA_PRIMARY_PROVIDER","MARKET_DATA_DEFAULT_TIMEFRAME","MARKET_DATA_ENABLE_STOCKS","MARKET_DATA_ENABLE_ETFS","MARKET_DATA_ENABLE_INDICES","ADMIN_ENV_SETTINGS_ENABLED"]
SECRETS = {"EODHD_API_KEY"}

def env_status():
    cfg=get_eodhd_config(); warnings=[]
    if cfg.enabled and not cfg.api_key: warnings.append("EODHD_ENABLED=true but EODHD_API_KEY is missing")
    if os.getenv("MARKET_DATA_PRIMARY_PROVIDER","EODHD")=="EODHD" and not cfg.enabled: warnings.append("MARKET_DATA_PRIMARY_PROVIDER=EODHD but EODHD_ENABLED=false")
    if cfg.max_concurrent > 10: warnings.append("EODHD_MAX_CONCURRENT may be too high")
    try: datetime.strptime(cfg.start_date, "%Y-%m-%d")
    except ValueError: warnings.append("EODHD_START_DATE must use YYYY-MM-DD")
    if os.getenv("ADMIN_ENV_SETTINGS_ENABLED","true").lower()=="false": warnings.append("ADMIN_ENV_SETTINGS_ENABLED=false")
    return {"variables":[{"name":k,"configured":bool(os.getenv(k)),"value":"***" if k in SECRETS and os.getenv(k) else os.getenv(k, ""),"secret":k in SECRETS} for k in ENV_VARS],"warnings":warnings,"editing_supported":False,"instructions":"Update secrets in Replit Secrets or deployment environment; runtime writes are intentionally not performed."}

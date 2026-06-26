import os
from signalmaker.data_providers.eodhd.config import get_eodhd_config

def market_data_settings(repo):
    cfg=get_eodhd_config(); stats=repo.stats()
    return {"primary_provider":os.getenv("MARKET_DATA_PRIMARY_PROVIDER","EODHD"),"eodhd_enabled":cfg.enabled,"eodhd_api_key_configured":bool(cfg.api_key),"default_timeframe":os.getenv("MARKET_DATA_DEFAULT_TIMEFRAME",cfg.default_timeframe),"default_exchange":cfg.default_exchange,"max_concurrent":cfg.max_concurrent,"request_sleep_seconds":cfg.request_sleep_seconds,"adjusted_data":cfg.adjusted_data,"start_date":cfg.start_date,**stats}

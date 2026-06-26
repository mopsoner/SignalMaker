import os
from signalmaker.data_providers.eodhd.config import get_eodhd_config
from signalmaker.data_providers.ibkr.config import get_ibkr_config

def market_data_settings(repo):
    cfg=get_eodhd_config(); ibkr=get_ibkr_config(); stats=repo.stats()
    primary=os.getenv("MARKET_DATA_PRIMARY_PROVIDER","EODHD").upper()
    default_timeframe=os.getenv("MARKET_DATA_DEFAULT_TIMEFRAME", ibkr.default_timeframe if primary == "IBKR" else cfg.default_timeframe)
    return {"primary_provider":primary,"eodhd_enabled":cfg.enabled,"eodhd_api_key_configured":bool(cfg.api_key),"ibkr_enabled":ibkr.enabled,"ibkr_auth_method":ibkr.auth_method,"ibkr_bearer_token_configured":bool(ibkr.bearer_token),"ibkr_oauth2_client_configured":bool(ibkr.oauth2_client_id and ibkr.oauth2_private_key),"default_timeframe":default_timeframe,"default_exchange":ibkr.default_exchange if primary == "IBKR" else cfg.default_exchange,"max_concurrent":ibkr.max_concurrent if primary == "IBKR" else cfg.max_concurrent,"request_sleep_seconds":ibkr.request_sleep_seconds if primary == "IBKR" else cfg.request_sleep_seconds,"adjusted_data":cfg.adjusted_data,"start_date":ibkr.start_date if primary == "IBKR" else cfg.start_date,"ibkr_history_period":ibkr.history_period,"ibkr_history_bar":ibkr.history_bar,"ibkr_use_rth":ibkr.use_regular_trading_hours,**stats}

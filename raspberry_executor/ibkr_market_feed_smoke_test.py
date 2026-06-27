import json, sys
from raspberry_executor.config import load_settings
from raspberry_executor.ibkr_client_portal import IBKRClientPortal

symbol = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
sec_type = sys.argv[2] if len(sys.argv) > 2 else "STK"
settings = load_settings(); cp = IBKRClientPortal(settings)
contracts = cp.search_contracts(symbol.split('.')[0], sec_type=sec_type)
conid = (contracts[0].get('conid') if contracts else None)
print(json.dumps({"auth": cp.auth_status(), "accounts_ok": bool(cp.list_accounts()), "symbol": symbol, "candidates": len(contracts), "conid": conid, "candles_sample": cp.historical_bars(conid, period="1m", bar="1d")[:3] if conid else []}, indent=2, default=str))

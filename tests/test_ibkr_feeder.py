import json
from pathlib import Path
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient
from app.main import app
from scripts.ibkr_feeder import (COLLECTION_PROFILE, build_payload, canonical_universe, filter_assets,
                                 load_assets, main, normalize_timestamp, parse_ibkr_bars,
                                 resolve_universe_assets, write_status)

ASSETS=[
 {"enabled":True,"symbol":"AIR.PA","provider_symbol":"AIR.PA","asset_type":"STOCK","region":"EU","country":"FR","currency":"EUR","exchange_code":"PA","universe":"Europe Stocks","pea_eligible":False,"ucits":False},
 {"enabled":True,"symbol":"ESE.PA","provider_symbol":"ESE.PA","asset_type":"ETF","region":"EU","currency":"EUR","exchange_code":"PA","universe":"Europe ETF","pea_eligible":True,"ucits":True},
 {"enabled":False,"symbol":"OFF.PA","provider_symbol":"OFF.PA","asset_type":"STOCK","region":"EU","currency":"EUR"},]
def filt(**kw):
 d={"include_disabled":False,"symbols":[],"provider_symbols":[],"max_assets":None,"pea_eligible":None,"ucits":None};d.update(kw);return d

def test_timestamps():
 assert normalize_timestamp(1754006400000)=="2025-08-01T00:00:00Z"
 assert normalize_timestamp(1754006400)=="2025-08-01T00:00:00Z"
 assert normalize_timestamp("2025-08-01T02:00:00+02:00")=="2025-08-01T00:00:00Z"
 assert normalize_timestamp("2025-08-01")=="2025-08-01T00:00:00Z"

def test_bar_shapes():
 short={"t":1754006400000,"o":1,"h":3,"l":.5,"c":2,"v":9}; long={"date":"2025-08-01","open":1,"high":3,"low":.5,"close":2,"volume":9}
 for key in ("data","bars","candles"): assert parse_ibkr_bars({key:[short]})[0]["close"]==2
 assert parse_ibkr_bars({"data":[long]})[0]["volume"]==9

def test_filters():
 for key,value,symbol in [("asset_type","STOCK","AIR.PA"),("asset_type","ETF","ESE.PA"),("region","EU","AIR.PA"),("currency","EUR","AIR.PA"),("exchange_code","PA","AIR.PA"),("universe","Europe ETF","ESE.PA"),("pea_eligible",True,"ESE.PA"),("ucits",True,"ESE.PA")]: assert filter_assets(ASSETS,filt(**{key:value}))[0]["symbol"]==symbol
 assert [a["symbol"] for a in filter_assets(ASSETS,filt(symbols=["AIR.PA","ESE.PA"]))]==["AIR.PA","ESE.PA"]
 assert len(filter_assets(ASSETS,filt(max_assets=1)))==1
 assert "OFF.PA" not in [a["symbol"] for a in filter_assets(ASSETS,filt())]

def test_payload_and_atomic_status(tmp_path):
 candles=parse_ibkr_bars({"data":[{"t":1754006400,"o":1,"h":2,"l":0,"c":1,"v":5}]}); payload=build_payload(ASSETS[0],candles)
 assert {"provider","symbol","candles","run_type","queue_analysis"} <= payload.keys(); assert payload["candles"][0]["timestamp"].endswith("Z")
 assert "operator_key" not in json.dumps(payload).lower()
 path=tmp_path/"new"/"status.json";write_status(path,{"run":{"status":"running"}});assert json.loads(path.read_text())["run"]["status"]=="running"

def test_monitoring_missing_status_and_logs(monkeypatch,tmp_path):
 import app.api.routes.ibkr_feeder as route
 monkeypatch.setattr(route,"_paths",lambda:(tmp_path/"missing.json",tmp_path/"assets.json",tmp_path/"missing.log"))
 client=TestClient(app); assert client.get('/api/ibkr-feeder/status').json()["run"]["status"]=="never_run";assert client.get('/api/ibkr-feeder/logs').json()["lines"]==[]

def test_missing_asset_config_is_actionable(monkeypatch,tmp_path,capsys):
 path=tmp_path/"missing.json"
 with patch.dict("os.environ",{"IBKR_FEEDER_ASSETS_FILE":str(path),"IBKR_FEEDER_STATUS_FILE":str(tmp_path/"status.json")}):
  assert main(["--universe", "Europe Stocks"])==2
 assert "cp config/ibkr_assets.example.json" in capsys.readouterr().err
 assert json.loads((tmp_path/"status.json").read_text())["run"]["status"]=="configuration_error"
 try: load_assets(path)
 except FileNotFoundError as exc: assert "IBKR asset configuration not found" in str(exc)
 else: raise AssertionError("missing configuration should fail")

def test_run_endpoint_rejects_missing_asset_config(monkeypatch,tmp_path):
 import app.api.routes.ibkr_feeder as route
 monkeypatch.setattr(route,"_paths",lambda:(tmp_path/"status.json",tmp_path/"assets.json",tmp_path/"log.txt"))
 response=TestClient(app).post('/api/ibkr-feeder/run-once',json={})
 assert response.status_code == 422
 body=TestClient(app).post('/api/ibkr-feeder/run-once',json={"universe":"Europe Stocks"}).json()
 assert body["ok"] is False and body["started"] is False
 assert "cp config/ibkr_assets.example.json" in body["message"]

def test_universe_validation_aliases_and_resolution(tmp_path):
 assert canonical_universe("Stocks Europe") == "Europe Stocks"
 assert canonical_universe("Stocks Euronext Paris") == "Europe Stocks"
 assert canonical_universe("ETF PEA") == "Europe ETF"
 assert canonical_universe("ETF Europe UCITS") == "Europe ETF"
 try: canonical_universe("Anything")
 except ValueError: pass
 else: raise AssertionError("unknown universe accepted")
 path=tmp_path/"assets.json"; path.write_text(json.dumps({"assets":ASSETS}))
 resolved=resolve_universe_assets(path,"Europe Stocks")
 assert [a["provider_symbol"] for a in resolved] == ["AIR.PA"]
 assert {"symbol","provider_symbol","asset_type","exchange_code","currency","region","country","isin","mic","pea_eligible","ucits"} <= resolved[0].keys()

def test_collection_profile_and_open_bar_exclusion():
 from datetime import datetime, timezone
 assert COLLECTION_PROFILE["shared_workflows"] == ("15m", "1h", "4h")
 response={"data":[{"t":1754006400,"o":1,"h":2,"l":0,"c":1,"v":5},{"t":1754007300,"o":1,"h":2,"l":0,"c":1,"v":5}]}
 closed=parse_ibkr_bars(response,"15m",lambda:datetime.fromtimestamp(1754008000,timezone.utc))
 assert len(closed)==1

def test_payload_uses_canonical_universe_and_no_per_batch_analysis():
 asset={**ASSETS[0],"universe":"Stocks Europe"}
 payload=build_payload(asset,[],"15m")
 assert payload["universe"] == "Europe Stocks" and payload["timeframe"] == "15m"
 assert payload["run_type"] == "universe_feed" and payload["queue_analysis"] is False

def test_auth_unavailable_and_unauthenticated():
 client=TestClient(app)
 with patch('app.api.routes.ibkr_feeder.requests.post',side_effect=OSError('offline')): assert client.post('/api/ibkr-feeder/check-auth').json()["reachable"] is False
 response=Mock();response.raise_for_status.return_value=None;response.json.return_value={"authenticated":False}
 with patch('app.api.routes.ibkr_feeder.requests.post',return_value=response):
  body=client.post('/api/ibkr-feeder/check-auth').json();assert body["reachable"] is True and body["ok"] is False

import json
from pathlib import Path
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient
from app.main import app
from scripts.ibkr_feeder import build_payload, filter_assets, load_assets, main, normalize_timestamp, parse_ibkr_bars, write_status

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
  assert main([])==2
 assert "cp config/ibkr_assets.example.json" in capsys.readouterr().err
 assert json.loads((tmp_path/"status.json").read_text())["run"]["status"]=="configuration_error"
 try: load_assets(path)
 except FileNotFoundError as exc: assert "IBKR asset configuration not found" in str(exc)
 else: raise AssertionError("missing configuration should fail")

def test_run_endpoint_rejects_missing_asset_config(monkeypatch,tmp_path):
 import app.api.routes.ibkr_feeder as route
 monkeypatch.setattr(route,"_paths",lambda:(tmp_path/"status.json",tmp_path/"assets.json",tmp_path/"log.txt"))
 body=TestClient(app).post('/api/ibkr-feeder/run-once',json={}).json()
 assert body["ok"] is False and body["started"] is False
 assert "cp config/ibkr_assets.example.json" in body["message"]

def test_auth_unavailable_and_unauthenticated():
 client=TestClient(app)
 with patch('app.api.routes.ibkr_feeder.requests.post',side_effect=OSError('offline')): assert client.post('/api/ibkr-feeder/check-auth').json()["reachable"] is False
 response=Mock();response.raise_for_status.return_value=None;response.json.return_value={"authenticated":False}
 with patch('app.api.routes.ibkr_feeder.requests.post',return_value=response):
  body=client.post('/api/ibkr-feeder/check-auth').json();assert body["reachable"] is True and body["ok"] is False

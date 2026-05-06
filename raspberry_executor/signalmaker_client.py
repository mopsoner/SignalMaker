import requests


class SignalMakerClient:
    def __init__(self, base_url: str, gateway_id: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.gateway_id = gateway_id
        self.session = requests.Session()

    def _url(self, path: str) -> str:
        if self.base_url.endswith("/api/v1") and path.startswith("/api/v1"):
            return f"{self.base_url}{path[len('/api/v1'):] }"
        return f"{self.base_url}{path}"

    def get_open_candidates(self, limit: int = 10) -> list[dict]:
        response = self.session.get(
            self._url("/api/v1/trade-candidates"),
            params={"status": "open", "limit": limit},
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected SignalMaker candidates response: {type(data).__name__}")
        return data

    def check_candle_ingest_endpoint(self) -> dict:
        probe = {
            "source": f"{self.gateway_id}-probe",
            "symbol": "BTCUSDT",
            "interval": "15m",
            "candles": [],
        }
        url = self._url("/api/v1/market-data/candles")
        response = self.session.post(url, json=probe, timeout=15)
        if response.status_code == 405:
            return {
                "ok": False,
                "status_code": 405,
                "url": url,
                "reason": "method_not_allowed_post_endpoint_missing",
                "message": "SignalMaker is reachable, but the deployed backend does not accept POST /api/v1/market-data/candles. Pull/redeploy main and restart Replit.",
            }
        if response.status_code == 404:
            return {
                "ok": False,
                "status_code": 404,
                "url": url,
                "reason": "endpoint_not_found",
                "message": "SignalMaker is reachable, but /api/v1/market-data/candles is missing. Pull/redeploy main and restart Replit.",
            }
        response.raise_for_status()
        return {"ok": True, "status_code": response.status_code, "url": url}

    def post_candles(self, symbol: str, interval: str, candles: list[dict], source: str | None = None) -> dict:
        payload = {
            "source": source or self.gateway_id,
            "symbol": symbol.upper(),
            "interval": interval,
            "candles": candles,
        }
        response = self.session.post(
            self._url("/api/v1/market-data/candles"),
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected SignalMaker candle ingest response: {type(data).__name__}")
        return data

    def candle_summary(self, symbol: str | None = None) -> list[dict]:
        params = {"symbol": symbol.upper()} if symbol else None
        response = self.session.get(
            self._url("/api/v1/market-data/candles/summary"),
            params=params,
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected SignalMaker candle summary response: {type(data).__name__}")
        return data

    def heartbeat(self, *args, **kwargs) -> dict:
        return {"status": "skipped", "reason": "local_mode_no_replit_gateway"}

    def report_execution(self, payload: dict) -> dict:
        return {"status": "skipped", "reason": "local_mode_no_replit_gateway"}

    def report_event(self, payload: dict) -> dict:
        return {"status": "skipped", "reason": "local_mode_no_replit_gateway"}

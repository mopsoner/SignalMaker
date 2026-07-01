import requests

from raspberry_executor.candidate_cursor_store import (
    advance_candidate_cursor,
    filter_candidates_after_cursor,
    read_candidate_cursor,
)
from raspberry_executor.local_candidate_store import list_local_candidates, upsert_remote_candidates


class SignalMakerClient:
    def __init__(self, base_url: str, gateway_id: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.gateway_id = gateway_id
        self.session = requests.Session()

    def _url(self, path: str) -> str:
        if self.base_url.endswith("/api/v1") and path.startswith("/api/v1"):
            return f"{self.base_url}{path[len('/api/v1'):]}"
        return f"{self.base_url}{path}"

    def get_admin_settings(self) -> dict:
        response = self.session.get(self._url("/api/v1/admin/settings"), timeout=10)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected SignalMaker admin settings response: {type(data).__name__}")
        return data

    def _candidate_params(self, limit: int, **extra) -> dict:
        params = {"limit": limit, **extra}
        cursor = read_candidate_cursor()
        if cursor:
            params["since"] = cursor
            params["created_after"] = cursor
            params["updated_after"] = cursor
        return params

    def _import_candidates(self, data: list[dict], limit: int) -> list[dict]:
        cursor = read_candidate_cursor()
        fresh = filter_candidates_after_cursor(data, cursor)
        if fresh:
            upsert_remote_candidates(fresh)
            advance_candidate_cursor(fresh)
        return list_local_candidates(limit=limit, include_executed=False)

    def get_open_candidates(self, limit: int = 10) -> list[dict]:
        response = self.session.get(
            self._url("/api/v1/trade-candidates"),
            params=self._candidate_params(limit, status="open"),
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected SignalMaker candidates response: {type(data).__name__}")
        return self._import_candidates(data, limit)

    def mark_candidate_executed(self, candidate_id: str) -> dict:
        # Deprecated for Raspberry local execution tracking. Kept for backwards
        # compatibility but intentionally does not call the remote server.
        return {"status": "skipped", "reason": "local_execution_tracking_only", "candidate_id": candidate_id}

    def get_recent_candidates(self, symbol: str | None = None, limit: int = 100) -> list[dict]:
        """Return local candidates refreshed from SignalMaker.

        The Raspberry now uses a local cursor, so it does not keep importing the
        same 50 old remote candidates after a local reset.
        """
        params = self._candidate_params(limit)
        if symbol:
            params["symbol"] = symbol.upper()

        attempts = [
            params,
            {**params, "status": "open"},
            {**params, "status": "new"},
        ]
        last_error: Exception | None = None
        for attempt_params in attempts:
            try:
                response = self.session.get(
                    self._url("/api/v1/trade-candidates"),
                    params=attempt_params,
                    timeout=15,
                )
                response.raise_for_status()
                data = response.json()
                if not isinstance(data, list):
                    raise RuntimeError(f"Unexpected SignalMaker candidates response: {type(data).__name__}")
                rows = self._import_candidates(data, limit)
                if symbol:
                    wanted = symbol.upper()
                    rows = [row for row in rows if str(row.get("symbol") or "").upper() == wanted]
                return rows
            except Exception as exc:
                last_error = exc
                continue
        if last_error:
            raise RuntimeError(f"Unable to fetch recent SignalMaker candidates: {last_error}")
        rows = list_local_candidates(limit=limit, include_executed=False)
        if symbol:
            wanted = symbol.upper()
            rows = [row for row in rows if str(row.get("symbol") or "").upper() == wanted]
        return rows

    def check_candle_ingest_endpoint(self) -> dict:
        probe = {
            "source": f"{self.gateway_id}-probe",
            "symbol": "BTCUSDT",
            "interval": "15m",
            "candles": [],
        }
        url = self._url("/api/v1/market-data/candles")
        try:
            response = self.session.post(url, json=probe, timeout=(5, 5))
        except requests.Timeout:
            return {
                "ok": False,
                "status_code": None,
                "url": url,
                "reason": "endpoint_timeout",
                "message": "SignalMaker timed out on the candle ingest probe. Pull/redeploy main, restart Replit, or reduce Replit load before enabling the candle feed.",
            }
        except requests.RequestException as exc:
            return {
                "ok": False,
                "status_code": None,
                "url": url,
                "reason": "endpoint_unreachable",
                "message": str(exc),
            }
        if response.status_code == 405:
            return {
                "ok": False,
                "status_code": 405,
                "url": url,
                "reason": "method_not_allowed_post_endpoint_missing",
                "message": "SignalMaker is reachable, but the deployed backend does not accept POST /api/v1/market-data/candles. Pull/redeploy main, restart Replit, or reduce Replit load before enabling the candle feed.",
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
            timeout=(5, 60),
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

    def latest_candle(self, symbol: str, interval: str) -> dict | None:
        response = self.session.get(
            self._url("/api/v1/market-data/candles"),
            params={"symbol": symbol.upper(), "interval": interval, "limit": 1, "latest": "true"},
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected SignalMaker latest candle response: {type(data).__name__}")
        return data[0] if data else None

    def first_candle(self, symbol: str, interval: str) -> dict | None:
        response = self.session.get(
            self._url("/api/v1/market-data/candles"),
            params={"symbol": symbol.upper(), "interval": interval, "limit": 1, "first": "true"},
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected SignalMaker first candle response: {type(data).__name__}")
        return data[0] if data else None

    def list_momentum(self, limit: int = 50) -> list[dict]:
        response = self.session.get(
            self._url("/api/v1/momentum"),
            params={"limit": limit},
            timeout=30,
            headers={"accept": "application/json", "cache-control": "no-cache"},
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected SignalMaker momentum response: {type(data).__name__}")
        return data

    def sync_momentum_candidates(self, limit: int = 25, min_momentum_score: float | None = None) -> dict:
        params: dict[str, object] = {"limit": limit}
        if min_momentum_score is not None:
            params["min_momentum_score"] = min_momentum_score
        response = self.session.post(
            self._url("/api/v1/executor/sync-momentum-candidates"),
            params=params,
            timeout=60,
            headers={"accept": "application/json"},
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected SignalMaker momentum candidate sync response: {type(data).__name__}")
        return data

    def heartbeat(self, *args, **kwargs) -> dict:
        return {"status": "skipped", "reason": "local_mode_no_replit_gateway"}

    def report_execution(self, payload: dict) -> dict:
        return {"status": "skipped", "reason": "local_mode_no_replit_gateway"}

    def report_event(self, payload: dict) -> dict:
        return {"status": "skipped", "reason": "local_mode_no_replit_gateway"}

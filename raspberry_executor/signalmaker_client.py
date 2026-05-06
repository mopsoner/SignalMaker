import requests


class SignalMakerClient:
    def __init__(self, base_url: str, gateway_id: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.gateway_id = gateway_id
        self.session = requests.Session()

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def get_open_candidates(self, limit: int = 10) -> list[dict]:
        response = self.session.get(
            self._url("/api/v1/trade-candidates"),
            params={"status": "open", "limit": limit},
            timeout=15,
        )
        response.raise_for_status()
        return response.json()

    def report_execution(self, payload: dict) -> dict:
        response = self.session.post(
            self._url("/api/v1/gateway/executions"),
            json=payload,
            timeout=20,
        )
        response.raise_for_status()
        return response.json()

    def report_event(self, payload: dict) -> dict:
        response = self.session.post(
            self._url("/api/v1/gateway/execution-events"),
            json=payload,
            timeout=20,
        )
        response.raise_for_status()
        return response.json()

    def heartbeat(self, *, mode: str, version: str = "0.1.0", meta: dict | None = None) -> dict:
        response = self.session.post(
            self._url("/api/v1/gateway/heartbeat"),
            json={
                "gateway_id": self.gateway_id,
                "status": "ok",
                "mode": mode,
                "version": version,
                "meta": meta or {},
            },
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

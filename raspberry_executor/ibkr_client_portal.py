from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any

import requests


class IBKRClientPortalError(RuntimeError):
    pass


class IBKRClientPortal:
    def __init__(self, settings) -> None:
        self.base_url = settings.ibkr_cp_base_url.rstrip("/")
        self.verify_ssl = bool(settings.ibkr_cp_verify_ssl)
        self.timeout = int(settings.ibkr_cp_timeout_seconds)
        self.session = requests.Session()
        if not self.verify_ssl:
            requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _request(self, method: str, path: str, **kwargs) -> Any:
        try:
            response = self.session.request(method, self._url(path), timeout=self.timeout, verify=self.verify_ssl, **kwargs)
        except requests.RequestException as exc:
            raise IBKRClientPortalError(f"IBKR CP request failed {method} {path}: {exc}") from exc
        if response.status_code in {401, 403}:
            raise IBKRClientPortalError(f"IBKR CP not authenticated/authorized ({response.status_code}) for {path}")
        if response.status_code == 429:
            raise IBKRClientPortalError(f"IBKR CP rate limited (429) for {path}")
        if response.status_code >= 500:
            raise IBKRClientPortalError(f"IBKR CP server error ({response.status_code}) for {path}")
        response.raise_for_status()
        return response.json() if response.content else {}

    def auth_status(self) -> dict[str, Any]:
        data = self._request("POST", "/iserver/auth/status")
        return data if isinstance(data, dict) else {"raw": data}

    def ensure_ready(self) -> dict[str, Any]:
        status = self.auth_status()
        if not bool(status.get("authenticated") or status.get("connected")):
            raise IBKRClientPortalError("ibkr_cp_not_authenticated")
        return status

    def list_accounts(self) -> Any:
        return self._request("GET", "/iserver/accounts")

    def search_contracts(self, symbol: str, sec_type: str | None = None) -> list[dict[str, Any]]:
        params = {"symbol": symbol}
        if sec_type:
            params["secType"] = sec_type
        data = self._request("GET", "/iserver/secdef/search", params=params)
        return data if isinstance(data, list) else data.get("data", []) if isinstance(data, dict) else []

    def contract_info(self, conid: int | str) -> dict[str, Any]:
        data = self._request("GET", f"/iserver/contract/{conid}/info")
        return data if isinstance(data, dict) else {"raw": data}

    def historical_bars(self, conid: int | str, period="2y", bar="1d", source="Last", outside_rth=False, exchange: str | None = None) -> list[dict[str, Any]]:
        params = {"conid": conid, "period": period, "bar": bar, "source": source, "outsideRth": str(outside_rth).lower()}
        if exchange:
            params["exchange"] = exchange
        data = self._request("GET", "/iserver/marketdata/history", params=params)
        rows = data.get("data", data) if isinstance(data, dict) else data
        return [self.normalize_bar(row, bar=bar) for row in (rows or []) if isinstance(row, dict)]

    @staticmethod
    def normalize_bar(row: dict[str, Any], bar: str = "1d") -> dict[str, Any]:
        raw_ts = int(row.get("t") or row.get("time") or row.get("timestamp"))
        open_time = raw_ts if raw_ts > 10_000_000_000 else raw_ts * 1000
        close_time = int(row.get("close_time") or row.get("ct") or 0)
        if not close_time:
            close_time = int((datetime.fromtimestamp(open_time / 1000, timezone.utc) + (timedelta(days=1) if bar == "1d" else timedelta(minutes=1))).timestamp() * 1000) - 1
        elif close_time < 10_000_000_000:
            close_time *= 1000
        return {
            "timestamp": datetime.fromtimestamp(open_time / 1000, timezone.utc).isoformat(),
            "open_time": open_time,
            "close_time": close_time,
            "open": float(row.get("o") or row.get("open")),
            "high": float(row.get("h") or row.get("high")),
            "low": float(row.get("l") or row.get("low")),
            "close": float(row.get("c") or row.get("close")),
            "volume": float(row.get("v") or row.get("volume") or 0),
        }

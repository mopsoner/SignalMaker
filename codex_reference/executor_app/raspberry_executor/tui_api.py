"""Shared HTTP helpers for Raspberry terminal UIs."""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from typing import Any


def default_base_url() -> str:
    port = os.getenv("EXECUTOR_API_PORT") or os.getenv("APP_PORT") or "8080"
    return f"http://127.0.0.1:{port}"


BASE_URL = os.getenv("SIGNALMAKER_BASE_URL", default_base_url()).rstrip("/")
TIMEOUT_SECONDS = int(os.getenv("SIGNALMAKER_TUI_TIMEOUT", "8") or "8")
USER_AGENT = "SignalMaker-Raspberry-TUI"


def api_request(path: str, params: dict[str, Any] | None = None, *, method: str = "GET") -> Any:
    qs = "?" + urllib.parse.urlencode(params) if params else ""
    url = f"{BASE_URL}{path}{qs}"
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw) if raw else {"status": resp.status}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"{path} -> HTTP {exc.code}: {body}") from exc
    except Exception as exc:
        raise RuntimeError(f"{path} unavailable: {exc}") from exc


def api_get(path: str, params: dict[str, Any] | None = None) -> Any:
    return api_request(path, params)


def as_rows(payload: Any) -> list[Any]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("items", "rows", "data", "results", "services", "workers", "candidates"):
            if isinstance(payload.get(key), list):
                return payload[key]
        return [
            {"name": k, **v} if isinstance(v, Mapping) else {"name": k, "value": v}
            for k, v in payload.items()
        ]
    return [{"value": payload}]

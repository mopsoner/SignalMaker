from __future__ import annotations

import base64
import hashlib
import hmac
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any, Protocol
from urllib.parse import urlencode

import requests


KRAKEN_BASE_URL = "https://api.kraken.com"
KRAKEN_PRIVATE_PREFIX = "/0/private/"
KRAKEN_REQUEST_TIMEOUT_SECONDS = 20


class HTTPResponse(Protocol):
    def raise_for_status(self) -> None: ...

    def json(self) -> Any: ...


class HTTPSession(Protocol):
    def post(
        self,
        url: str,
        *,
        data: str,
        headers: Mapping[str, str],
        timeout: int,
    ) -> HTTPResponse: ...


class KrakenNonceGenerator:
    """Generate monotonically increasing millisecond nonces, safely across threads."""

    def __init__(self, time_ms: Callable[[], int] | None = None) -> None:
        self._time_ms = time_ms or (lambda: time.time_ns() // 1_000_000)
        self._previous_nonce = 0
        self._lock = threading.Lock()

    def __call__(self) -> int:
        with self._lock:
            nonce = max(int(self._time_ms()), self._previous_nonce + 1)
            self._previous_nonce = nonce
            return nonce


class KrakenAPIError(RuntimeError):
    """An error returned in an otherwise successful Kraken API response."""

    def __init__(self, path: str, errors: list[str] | tuple[str, ...]) -> None:
        self.path = path
        self.errors = tuple(str(error) for error in errors)
        super().__init__(f"Kraken POST {path} failed errors={list(self.errors)!r}")


class KrakenClient:
    """Minimal Kraken Spot client for authenticated private REST requests."""

    exchange_name = "kraken"

    def __init__(
        self,
        base_url: str = KRAKEN_BASE_URL,
        api_key: str = "",
        secret_key: str = "",
        dry_run: bool = True,
        *,
        session: HTTPSession | None = None,
        nonce_provider: Callable[[], int | str] | None = None,
    ) -> None:
        self.base_url = (base_url or KRAKEN_BASE_URL).rstrip("/")
        self.api_key = api_key
        self.secret_key = secret_key
        self.dry_run = dry_run
        self.session = session or requests.Session()
        self._nonce_provider = nonce_provider or KrakenNonceGenerator()

    def is_configured(self) -> bool:
        return bool(self.api_key and self.secret_key)

    def recv_window_ms(self) -> int:
        # Kraken private requests use nonces rather than a receive window.
        return 0

    def _signed(
        self,
        method: str,
        path: str,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        if method.upper() != "POST":
            raise ValueError("Kraken private REST endpoints require POST")
        if not path.startswith(KRAKEN_PRIVATE_PREFIX):
            raise ValueError(f"Kraken private REST path must start with {KRAKEN_PRIVATE_PREFIX}")
        if not self.is_configured():
            raise RuntimeError("Kraken API credentials are missing")

        nonce = str(self._nonce_provider())
        payload = dict(params or {})
        payload["nonce"] = nonce
        encoded_body = urlencode(payload)
        message = path.encode() + hashlib.sha256((nonce + encoded_body).encode()).digest()
        signature = hmac.new(
            base64.b64decode(self.secret_key),
            message,
            hashlib.sha512,
        ).digest()
        headers = {
            "API-Key": self.api_key,
            "API-Sign": base64.b64encode(signature).decode(),
            "Content-Type": "application/x-www-form-urlencoded",
        }

        response = self.session.post(
            f"{self.base_url}{path}",
            data=encoded_body,
            headers=headers,
            timeout=KRAKEN_REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()
        errors = data.get("error") or []
        if errors:
            raise KrakenAPIError(path, errors)
        return data.get("result") or {}

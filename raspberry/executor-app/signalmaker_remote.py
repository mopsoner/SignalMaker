from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import requests

LOGGER = logging.getLogger("signalmaker.executor.remote_api")
MOMENTUM_CANDIDATES_ENDPOINT = "/api/v1/momentum-candidates"
HEALTH_ENDPOINT = "/api/v1/health"
HTML_BODY_PREVIEW_CHARS = 500


@dataclass(frozen=True)
class ApiConfig:
    base_url: str
    momentum_candidates_sync_enabled: bool = True
    timeout_seconds: float = 15.0


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def validate_api_base_url(raw_base_url: str | None) -> str:
    base_url = (raw_base_url or "").strip().rstrip("/")
    LOGGER.info("API_BASE=%s", base_url or "<empty>")

    if not base_url:
        raise ValueError("API_BASE is required and cannot be empty")
    if "mysginalmaker" in base_url.lower():
        LOGGER.error(
            "invalid_api_base_typo API_BASE=%s expected_spelling=mysignalmaker",
            base_url,
        )
        raise ValueError("API_BASE appears to contain the typo 'mysginalmaker'")
    if not base_url.startswith(("http://", "https://")):
        raise ValueError("API_BASE must start with http:// or https://")

    return base_url


def load_api_config() -> ApiConfig:
    return ApiConfig(
        base_url=validate_api_base_url(os.getenv("API_BASE")),
        momentum_candidates_sync_enabled=env_bool("MOMENTUM_CANDIDATES_SYNC_ENABLED", True),
        timeout_seconds=float(os.getenv("SIGNALMAKER_API_TIMEOUT_SECONDS", "15")),
    )


def api_url(base_url: str, path: str) -> str:
    return urljoin(f"{base_url.rstrip('/')}/", path.lstrip("/"))


def _body_preview(response: requests.Response) -> str:
    return response.text[:HTML_BODY_PREVIEW_CHARS].replace("\n", " ").replace("\r", " ")


def _log_html_response(response: requests.Response) -> None:
    LOGGER.error(
        "remote_api_returned_html status_code=%s content_type=%s url=%s body_preview=%s",
        response.status_code,
        response.headers.get("content-type", ""),
        response.url,
        _body_preview(response),
    )


def response_is_html(response: requests.Response) -> bool:
    content_type = response.headers.get("content-type", "").lower()
    text_start = response.text.lstrip()[:64].lower()
    return "text/html" in content_type or text_start.startswith(("<!doctype html", "<html"))


def fetch_json(session: requests.Session, url: str, *, timeout_seconds: float) -> Any | None:
    response = session.get(url, timeout=timeout_seconds)
    content_type = response.headers.get("content-type", "")

    if response_is_html(response):
        _log_html_response(response)
        return None

    if "application/json" not in content_type.lower():
        LOGGER.error(
            "remote_api_non_json_response status_code=%s content_type=%s url=%s body_preview=%s",
            response.status_code,
            content_type,
            response.url,
            _body_preview(response),
        )
        return None

    try:
        return response.json()
    except json.JSONDecodeError:
        LOGGER.error(
            "remote_api_invalid_json status_code=%s content_type=%s url=%s body_preview=%s",
            response.status_code,
            content_type,
            response.url,
            _body_preview(response),
        )
        return None


def run_startup_api_checks(config: ApiConfig, session: requests.Session | None = None) -> None:
    active_session = session or requests.Session()
    health_url = api_url(config.base_url, HEALTH_ENDPOINT)
    candidates_url = api_url(config.base_url, f"{MOMENTUM_CANDIDATES_ENDPOINT}?limit=1")

    fetch_json(active_session, health_url, timeout_seconds=config.timeout_seconds)
    fetch_json(active_session, candidates_url, timeout_seconds=config.timeout_seconds)


def fetch_momentum_candidates(
    config: ApiConfig,
    session: requests.Session | None = None,
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    if not config.momentum_candidates_sync_enabled:
        LOGGER.info("momentum_candidates_sync_disabled")
        return []

    active_session = session or requests.Session()
    candidates_url = api_url(config.base_url, f"{MOMENTUM_CANDIDATES_ENDPOINT}?limit={limit}")
    payload = fetch_json(active_session, candidates_url, timeout_seconds=config.timeout_seconds)

    if payload is None:
        return []
    if not isinstance(payload, list):
        LOGGER.error("remote_api_unexpected_payload url=%s payload_type=%s", candidates_url, type(payload).__name__)
        return []

    return [item for item in payload if isinstance(item, dict)]

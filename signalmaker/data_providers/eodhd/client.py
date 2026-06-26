import httpx

from .config import EODHDConfig
from .errors import EODHDDisabledError, EODHDMissingApiKeyError, EODHDRequestError


class EODHDClient:
    def __init__(self, config: EODHDConfig):
        self.config = config
        if not config.enabled:
            raise EODHDDisabledError("EODHD provider disabled. Set EODHD_ENABLED=true.")
        if not config.api_key:
            raise EODHDMissingApiKeyError("Missing EODHD_API_KEY.")
        self.client = httpx.AsyncClient(timeout=30)

    async def close(self):
        await self.client.aclose()

    async def get_json(self, path: str, params: dict | None = None):
        request_params = dict(params or {})
        request_params["api_token"] = self.config.api_key
        request_params["fmt"] = "json"
        url = f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"
        response = await self.client.get(url, params=request_params)
        if response.status_code >= 400:
            raise EODHDRequestError(
                f"EODHD request failed status={response.status_code} body={response.text[:500]}"
            )
        try:
            return response.json()
        except ValueError as exc:
            raise EODHDRequestError("EODHD response was not valid JSON") from exc

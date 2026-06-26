import httpx

from .auth import IBKROAuth2PrivateKeyJWT
from .config import IBKRConfig
from .errors import IBKRAuthConfigurationError, IBKRDisabledError, IBKRMissingTokenError, IBKRRequestError


class IBKRClient:
    def __init__(self, config: IBKRConfig):
        self.config = config
        if not config.enabled:
            raise IBKRDisabledError("IBKR provider disabled. Set IBKR_ENABLED=true.")
        if config.auth_method not in {"gateway", "bearer", "oauth2_private_key_jwt"}:
            raise IBKRAuthConfigurationError(
                "IBKR_AUTH_METHOD must be one of: gateway, bearer, oauth2_private_key_jwt"
            )
        if config.auth_method == "bearer" and not config.bearer_token:
            raise IBKRMissingTokenError("Missing IBKR_BEARER_TOKEN for IBKR_AUTH_METHOD=bearer.")
        self.client = httpx.AsyncClient(timeout=30)
        self.oauth2 = IBKROAuth2PrivateKeyJWT(config) if config.auth_method == "oauth2_private_key_jwt" else None

    async def close(self):
        await self.client.aclose()

    def _url(self, path: str) -> str:
        base = self.config.base_url.rstrip("/")
        api_path = self.config.trading_base_path.strip("/")
        return f"{base}/{api_path}/{path.lstrip('/')}"

    async def _auth_headers(self) -> dict[str, str]:
        if self.config.auth_method == "gateway":
            return {}
        if self.config.auth_method == "bearer":
            return {"Authorization": f"Bearer {self.config.bearer_token}"}
        if self.oauth2 is None:
            raise IBKRAuthConfigurationError("IBKR OAuth2 authentication was not initialized.")
        return await self.oauth2.authorization_header(self.client)

    async def get_json(self, path: str, params: dict | None = None):
        response = await self.client.get(
            self._url(path),
            params=dict(params or {}),
            headers=await self._auth_headers(),
        )
        if response.status_code >= 400:
            raise IBKRRequestError(
                f"IBKR request failed status={response.status_code} body={response.text[:500]}"
            )
        try:
            return response.json()
        except ValueError as exc:
            raise IBKRRequestError("IBKR response was not valid JSON") from exc

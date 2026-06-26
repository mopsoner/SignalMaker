from __future__ import annotations

from dataclasses import dataclass
from time import time
from uuid import uuid4

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import httpx

from .config import IBKRConfig
from .errors import IBKRAuthConfigurationError, IBKRRequestError

PRIVATE_KEY_JWT_ASSERTION_TYPE = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"


@dataclass
class IBKRAccessToken:
    access_token: str
    token_type: str = "Bearer"
    expires_at: float | None = None
    scope: str | None = None

    def is_valid(self, skew_seconds: int = 60) -> bool:
        return bool(self.access_token) and (self.expires_at is None or self.expires_at - skew_seconds > time())


class IBKROAuth2PrivateKeyJWT:
    """OAuth 2.0 token client for IBKR's private_key_jwt client authentication.

    IBKR documents OAuth 2.0 as an institutional/beta direct-connect method and
    states that clients authenticate at the token endpoint with a signed JWT
    client_assertion instead of a client secret.
    """

    def __init__(self, config: IBKRConfig):
        self.config = config
        self._token: IBKRAccessToken | None = None

    async def authorization_header(self, http_client: "httpx.AsyncClient") -> dict[str, str]:
        token = await self.get_access_token(http_client)
        return {"Authorization": f"{token.token_type} {token.access_token}"}

    async def get_access_token(self, http_client: "httpx.AsyncClient", *, force_refresh: bool = False) -> IBKRAccessToken:
        if not force_refresh and self._token and self._token.is_valid():
            return self._token
        self._validate_config()
        assertion = self._build_client_assertion()
        data = {
            "grant_type": self.config.oauth2_grant_type,
            "client_id": self.config.oauth2_client_id,
            "client_assertion_type": PRIVATE_KEY_JWT_ASSERTION_TYPE,
            "client_assertion": assertion,
        }
        if self.config.oauth2_scope:
            data["scope"] = self.config.oauth2_scope
        response = await http_client.post(self.config.oauth2_token_url, data=data)
        if response.status_code >= 400:
            raise IBKRRequestError(
                f"IBKR OAuth2 token request failed status={response.status_code} body={response.text[:500]}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise IBKRRequestError("IBKR OAuth2 token response was not valid JSON") from exc
        access_token = payload.get("access_token")
        if not access_token:
            raise IBKRRequestError("IBKR OAuth2 token response did not include access_token")
        expires_in = payload.get("expires_in")
        self._token = IBKRAccessToken(
            access_token=access_token,
            token_type=payload.get("token_type") or "Bearer",
            expires_at=time() + int(expires_in) if expires_in is not None else None,
            scope=payload.get("scope"),
        )
        return self._token

    def _validate_config(self) -> None:
        missing = []
        if not self.config.oauth2_client_id:
            missing.append("IBKR_OAUTH2_CLIENT_ID")
        if not self.config.oauth2_private_key:
            missing.append("IBKR_OAUTH2_PRIVATE_KEY or IBKR_OAUTH2_PRIVATE_KEY_FILE")
        if not self.config.oauth2_token_url:
            missing.append("IBKR_OAUTH2_TOKEN_URL")
        if missing:
            raise IBKRAuthConfigurationError("Missing IBKR OAuth2 configuration: " + ", ".join(missing))

    def _build_client_assertion(self) -> str:
        try:
            import jwt
        except ImportError as exc:
            raise IBKRAuthConfigurationError(
                "IBKR OAuth2 private_key_jwt requires PyJWT with crypto support. Install requirements.txt."
            ) from exc
        now = int(time())
        headers = {"alg": self.config.oauth2_jwt_algorithm, "typ": "JWT"}
        if self.config.oauth2_key_id:
            headers["kid"] = self.config.oauth2_key_id
        claims = {
            "iss": self.config.oauth2_client_id,
            "sub": self.config.oauth2_client_id,
            "aud": self.config.oauth2_token_url,
            "jti": str(uuid4()),
            "iat": now,
            "exp": now + self.config.oauth2_assertion_ttl_seconds,
        }
        return jwt.encode(
            claims,
            self.config.oauth2_private_key,
            algorithm=self.config.oauth2_jwt_algorithm,
            headers=headers,
        )

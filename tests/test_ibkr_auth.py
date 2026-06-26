import pytest

from signalmaker.data_providers.ibkr.auth import IBKROAuth2PrivateKeyJWT
from signalmaker.data_providers.ibkr.config import IBKRConfig
from signalmaker.data_providers.ibkr.errors import IBKRAuthConfigurationError


def ibkr_config(**overrides):
    values = dict(
        enabled=True,
        auth_method="gateway",
        bearer_token="",
        base_url="https://api.ibkr.com",
        trading_base_path="/v1/api",
        default_exchange="SMART",
        default_timeframe="1d",
        request_sleep_seconds=1.0,
        max_concurrent=2,
        start_date="2020-01-01",
        history_period="5y",
        history_bar="1d",
        use_regular_trading_hours=True,
        oauth2_token_url="https://api.ibkr.com/oauth2/api/v1/token",
        oauth2_client_id="",
        oauth2_private_key="",
        oauth2_key_id="",
        oauth2_scope="",
        oauth2_grant_type="client_credentials",
        oauth2_jwt_algorithm="RS256",
        oauth2_assertion_ttl_seconds=300,
    )
    values.update(overrides)
    return IBKRConfig(**values)


@pytest.mark.parametrize("method", ["gateway", "bearer", "oauth2_private_key_jwt"])
def test_ibkr_config_supports_expected_auth_methods(method):
    assert ibkr_config(auth_method=method).auth_method == method


def test_oauth2_private_key_jwt_validates_required_registration_values():
    auth = IBKROAuth2PrivateKeyJWT(ibkr_config(auth_method="oauth2_private_key_jwt"))
    with pytest.raises(IBKRAuthConfigurationError, match="IBKR_OAUTH2_CLIENT_ID"):
        auth._validate_config()


def test_oauth2_private_key_jwt_accepts_registered_client_values():
    auth = IBKROAuth2PrivateKeyJWT(
        ibkr_config(
            auth_method="oauth2_private_key_jwt",
            oauth2_client_id="client-id",
            oauth2_private_key="-----BEGIN PRIVATE KEY-----\nplaceholder\n-----END PRIVATE KEY-----",
        )
    )
    auth._validate_config()

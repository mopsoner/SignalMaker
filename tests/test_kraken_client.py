import base64
import hashlib
import hmac
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock
from urllib.parse import parse_qs

import pytest

from app.services.execution.kraken_client import (
    KrakenAPIError,
    KrakenClient,
    KrakenNonceGenerator,
)


def test_nonce_is_monotonic_when_clock_does_not_advance():
    nonce = KrakenNonceGenerator(lambda: 1000)

    with ThreadPoolExecutor(max_workers=8) as pool:
        values = list(pool.map(lambda _: nonce(), range(20)))

    assert sorted(values) == list(range(1000, 1020))
    assert len(set(values)) == len(values)


def test_signed_request_uses_exact_path_body_nonce_and_signature():
    secret = base64.b64encode(b"test secret").decode()
    response = Mock()
    response.json.return_value = {"error": [], "result": {"ok": True}}
    session = Mock()
    session.post.return_value = response
    client = KrakenClient(
        "https://kraken.invalid/",
        "public-key",
        secret,
        session=session,
        nonce_provider=lambda: 123456,
    )

    assert client._signed("POST", "/0/private/Balance", {"asset": "XBT USD"}) == {"ok": True}

    url = session.post.call_args.args[0]
    kwargs = session.post.call_args.kwargs
    assert url == "https://kraken.invalid/0/private/Balance"
    assert parse_qs(kwargs["data"]) == {"asset": ["XBT USD"], "nonce": ["123456"]}
    digest = hashlib.sha256(("123456" + kwargs["data"]).encode()).digest()
    expected = base64.b64encode(
        hmac.new(b"test secret", b"/0/private/Balance" + digest, hashlib.sha512).digest()
    ).decode()
    assert kwargs["headers"] == {
        "API-Key": "public-key",
        "API-Sign": expected,
        "Content-Type": "application/x-www-form-urlencoded",
    }
    assert kwargs["timeout"] == 20
    response.raise_for_status.assert_called_once_with()


def test_kraken_error_is_structured_and_does_not_expose_credentials():
    response = Mock()
    response.json.return_value = {"error": ["EAPI:Invalid nonce"]}
    session = Mock()
    session.post.return_value = response
    secret = base64.b64encode(b"never display me").decode()
    client = KrakenClient(api_key="never display key", secret_key=secret, session=session)

    with pytest.raises(KrakenAPIError) as caught:
        client._signed("POST", "/0/private/Balance")

    assert caught.value.path == "/0/private/Balance"
    assert caught.value.errors == ("EAPI:Invalid nonce",)
    message = str(caught.value)
    assert "never display" not in message
    assert secret not in message
    assert session.post.call_args.kwargs["headers"]["API-Sign"] not in message


def test_signed_rejects_non_private_path_without_sending_request():
    session = Mock()
    client = KrakenClient(
        api_key="key",
        secret_key=base64.b64encode(b"secret").decode(),
        session=session,
    )

    with pytest.raises(ValueError, match="private REST path"):
        client._signed("POST", "/private/Balance")

    session.post.assert_not_called()

from __future__ import annotations

import logging
import unittest
from unittest.mock import Mock

from signalmaker_remote import api_url, fetch_json, validate_api_base_url


class FakeResponse:
    def __init__(self, text: str, content_type: str, status_code: int = 200, url: str = "https://api.example.test/path") -> None:
        self.text = text
        self.headers = {"content-type": content_type}
        self.status_code = status_code
        self.url = url

    def json(self):
        return {"status": "ok"}


class SignalMakerRemoteTests(unittest.TestCase):
    def test_validate_base_url_rejects_empty(self) -> None:
        with self.assertRaises(ValueError):
            validate_api_base_url("")

    def test_validate_base_url_rejects_known_typo(self) -> None:
        with self.assertRaises(ValueError):
            validate_api_base_url("https://mysginalmaker.replit.app")

    def test_api_url_uses_momentum_candidates_path(self) -> None:
        self.assertEqual(
            api_url("https://example.test/", "/api/v1/momentum-candidates?limit=1"),
            "https://example.test/api/v1/momentum-candidates?limit=1",
        )

    def test_fetch_json_logs_html_without_crashing(self) -> None:
        session = Mock()
        session.get.return_value = FakeResponse("<html>frontend</html>", "text/html; charset=utf-8")

        with self.assertLogs("signalmaker.executor.remote_api", level=logging.ERROR) as captured:
            result = fetch_json(session, "https://example.test/api/v1/momentum-candidates?limit=1", timeout_seconds=1)

        self.assertIsNone(result)
        self.assertIn("remote_api_returned_html", "\n".join(captured.output))


if __name__ == "__main__":
    unittest.main()

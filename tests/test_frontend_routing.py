from pathlib import Path
import sys

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.main import app


def test_ops_html_serves_admin_page() -> None:
    client = TestClient(app)

    response = client.get("/ops.html")

    assert response.status_code == 200
    assert "Settings / Logs" in response.text
    assert "admin-settings-content" in response.text


def test_legacy_admin_urls_redirect_to_ops() -> None:
    client = TestClient(app, follow_redirects=False)

    for path in ["/admin", "/admin/", "/admin.html", "/settings.html", "/logs.html", "/feed.html"]:
        response = client.get(path)
        assert response.status_code == 307
        assert response.headers["location"] == "/ops.html"

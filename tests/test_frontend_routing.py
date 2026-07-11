from pathlib import Path
import sys

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.main import app

ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = ROOT / "frontend"
DIST_DIR = FRONTEND_DIR / "dist"
FRONTEND_BUILD_COMMAND = "bash scripts/build_frontend.sh"


def test_frontend_dist_matches_sources() -> None:
    source_files = [
        FRONTEND_DIR / "app.js",
        FRONTEND_DIR / "styles.css",
        *sorted(FRONTEND_DIR.glob("*.html")),
    ]

    mismatches: list[str] = []
    for source_path in source_files:
        dist_path = DIST_DIR / source_path.name
        if not dist_path.exists():
            mismatches.append(f"missing {dist_path.relative_to(ROOT)}")
            continue
        if source_path.read_bytes() != dist_path.read_bytes():
            mismatches.append(
                f"{source_path.relative_to(ROOT)} differs from {dist_path.relative_to(ROOT)}"
            )

    assert not mismatches, (
        "frontend/dist is out of sync with frontend sources: "
        + "; ".join(mismatches)
        + f". Run `{FRONTEND_BUILD_COMMAND}` to refresh the checked-in static build."
    )


def test_documented_frontend_build_command_is_supported() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    assert FRONTEND_BUILD_COMMAND in readme
    assert (ROOT / "scripts" / "build_frontend.sh").is_file()
    assert (FRONTEND_DIR / "build.js").is_file()


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

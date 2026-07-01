import sys
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.api import deps
from app.api.routes import admin_settings
from app.main import app


class FakeDb:
    pass


def _client(monkeypatch):
    fake_db = FakeDb()

    def override_get_db():
        yield fake_db

    app.dependency_overrides[deps.get_db] = override_get_db
    monkeypatch.setattr(
        admin_settings,
        "load_admin_settings",
        lambda db, include_sources=False: {"executor": {"quote_assets": "USD"}},
    )
    saved = []

    def fake_persist(db, payload):
        saved.append(payload)
        return payload

    monkeypatch.setattr(admin_settings, "persist_runtime_settings", fake_persist)
    client = TestClient(app)
    return client, saved


def teardown_function():
    app.dependency_overrides.clear()


def test_admin_settings_save_does_not_require_token(monkeypatch):
    client, saved = _client(monkeypatch)
    payload = {"executor": {"quote_assets": "USD"}}

    response = client.put("/api/v1/admin/settings", json=payload)

    assert response.status_code == 200
    assert saved == [{**payload, "general": {}, "kraken": {}, "market_data": {}, "strategy": {}, "notifications": {}, "bot": {}, "live": {}, "momentum": {}}]
    assert response.json() == {"executor": {"quote_assets": "USD"}}


def test_admin_worker_status_and_logs_do_not_require_token(monkeypatch):
    client, _ = _client(monkeypatch)

    class FakeWorkerControlService:
        def status(self):
            return {"workers": []}

    monkeypatch.setattr(admin_settings, "WorkerControlService", FakeWorkerControlService)

    for path in [
        "/api/v1/admin/workers",
        "/api/v1/admin/logs/pipeline",
    ]:
        response = client.get(path)

        assert response.status_code == 200, path


def test_worker_logs_rejects_too_many_lines(monkeypatch):
    client, _ = _client(monkeypatch)

    response = client.get("/api/v1/admin/logs/pipeline?lines=1001")

    assert response.status_code == 422

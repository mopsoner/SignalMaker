import sys
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.api import deps
from app.api.routes import admin_settings
from app.main import app


class FakeDb:
    pass


def _client(monkeypatch, token="valid-token"):
    fake_db = FakeDb()

    def override_get_db():
        yield fake_db

    app.dependency_overrides[deps.get_db] = override_get_db
    monkeypatch.setattr(deps, "load_runtime_settings", lambda db: {"general": {"admin_token": token}})
    monkeypatch.setattr(
        admin_settings,
        "load_admin_settings",
        lambda db, include_sources=False: {"general": {"admin_token": token}},
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


def test_missing_operator_key_blocks_sensitive_admin_routes(monkeypatch):
    client, _ = _client(monkeypatch)

    for method, path in [
        ("get", "/api/v1/admin/settings"),
        ("put", "/api/v1/admin/settings"),
        ("post", "/api/v1/admin/reset-database"),
        ("post", "/api/v1/admin/test/kraken"),
        ("post", "/api/v1/admin/test/notifications"),
        ("get", "/api/v1/admin/workers"),
        ("get", "/api/v1/admin/logs/pipeline"),
        ("post", "/api/v1/admin/workers/pipeline/start"),
        ("post", "/api/v1/admin/workers/pipeline/stop"),
    ]:
        if method == "put":
            response = client.put(path, json={})
        else:
            response = getattr(client, method)(path)
        assert response.status_code == 401, path
        assert response.json()["detail"] == "Token admin invalide ou manquant"


def test_invalid_operator_key_blocks_sensitive_admin_routes(monkeypatch):
    client, _ = _client(monkeypatch)

    response = client.put(
        "/api/v1/admin/settings",
        headers={"x-operator-key": "wrong-token"},
        json={"general": {"admin_token": "valid-token"}},
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Token admin invalide ou manquant"


def test_valid_operator_key_allows_admin_settings_save(monkeypatch):
    client, saved = _client(monkeypatch)
    payload = {"general": {"admin_token": "valid-token"}, "executor": {"quote_assets": "USD"}}

    response = client.put(
        "/api/v1/admin/settings",
        headers={"x-operator-key": "valid-token"},
        json=payload,
    )

    assert response.status_code == 200
    assert saved == [{**payload, "kraken": {}, "market_data": {}, "strategy": {}, "notifications": {}, "bot": {}, "live": {}, "momentum": {}}]
    assert response.json() == {"general": {"admin_token": "valid-token"}}


def test_valid_operator_key_allows_worker_status_and_logs(monkeypatch):
    client, _ = _client(monkeypatch)

    class FakeWorkerControlService:
        def status(self):
            return {"workers": []}

    monkeypatch.setattr(admin_settings, "WorkerControlService", FakeWorkerControlService)

    for path in [
        "/api/v1/admin/workers",
        "/api/v1/admin/logs/pipeline",
    ]:
        response = client.get(path, headers={"x-operator-key": "valid-token"})

        assert response.status_code == 200, path

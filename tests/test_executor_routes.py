from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.api.deps import get_db
from app.api.routes import executor as route
from app.main import app


class RecordingExecutor:
    calls: list[dict] = []

    def __init__(self, _db):
        pass

    def execute_open_candidates(self, **kwargs):
        self.calls.append(kwargs)
        return {"mode": kwargs["mode"], "executed": [], "skipped": []}


def client(monkeypatch) -> TestClient:
    RecordingExecutor.calls = []
    monkeypatch.setattr(route, "ExecutorService", RecordingExecutor)
    app.dependency_overrides[get_db] = lambda: object()
    return TestClient(app)


def test_paper_route_always_executes_paper_even_with_mode_query(monkeypatch):
    response = client(monkeypatch).post("/api/v1/executor/run-once?mode=live")

    assert response.status_code == 200
    assert RecordingExecutor.calls == [{"limit": 10, "quantity": 1.0, "mode": "paper"}]


def test_live_route_rejects_unauthenticated_request_before_execution(monkeypatch):
    response = client(monkeypatch).post(
        "/api/v1/executor/live/run-once",
        headers={"X-Confirm-Live-Execution": "EXECUTE-WYCKOFF-LIVE"},
    )

    assert response.status_code == 401
    assert RecordingExecutor.calls == []


def test_live_route_rejects_unsafe_configuration_before_execution(monkeypatch):
    test_client = client(monkeypatch)
    monkeypatch.setattr(
        route,
        "settings",
        SimpleNamespace(
            admin_token="changeme-admin-token",
            wyckoff_live_enabled=True,
            wyckoff_live_mode="spot",
            kraken_execution_enabled=True,
            kraken_dry_run=True,
            kraken_api_key="key",
            kraken_secret_key="secret",
            kraken_margin_execution_enabled=False,
        ),
    )
    response = test_client.post(
        "/api/v1/executor/live/run-once",
        headers={
            "X-Operator-Key": "changeme-admin-token",
            "X-Confirm-Live-Execution": "EXECUTE-WYCKOFF-LIVE",
        },
    )

    assert response.status_code == 503
    assert "KRAKEN_DRY_RUN must be false" in response.json()["detail"]
    assert RecordingExecutor.calls == []


def test_selected_live_candidate_route_executes_only_requested_candidate(monkeypatch):
    test_client = client(monkeypatch)
    monkeypatch.setattr(route, "assert_wyckoff_live_configuration", lambda _settings: None)
    response = test_client.post(
        "/api/v1/executor/live/candidates/BTCUSD-open?quantity=2.5",
        headers={
            "X-Operator-Key": "changeme-admin-token",
            "X-Confirm-Live-Execution": "EXECUTE-WYCKOFF-LIVE",
        },
    )

    assert response.status_code == 200
    assert RecordingExecutor.calls == [{
        "limit": 1,
        "quantity": 2.5,
        "mode": "live",
        "candidate_id": "BTCUSD-open",
    }]

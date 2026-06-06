from app.api.routes import momentum_engine as momentum_engine_route


def test_momentum_engine_decision_route_is_registered() -> None:
    route = next(
        route
        for route in momentum_engine_route.router.routes
        if getattr(route, "path", None) == "/decision"
    )

    assert "GET" in route.methods


def test_momentum_engine_decision_route_returns_executor_contract(monkeypatch) -> None:
    captured = {}

    class FakeMomentumEngineService:
        def __init__(self, db) -> None:
            captured["db"] = db

        def status(self, *, cadence_hours: int, starting_capital: float, min_momentum_score: float) -> dict:
            captured["params"] = {
                "cadence_hours": cadence_hours,
                "starting_capital": starting_capital,
                "min_momentum_score": min_momentum_score,
            }
            return {
                "strategy": "momentum_rotation_v1",
                "recommendation": "Buy ETHUSDC",
                "due_now": True,
                "last_check_at": None,
                "next_check_at": None,
                "open_position": None,
                "best_asset": {"symbol": "ETHUSDC", "rank": 1},
            }

    monkeypatch.setattr(momentum_engine_route, "MomentumEngineService", FakeMomentumEngineService)

    payload = momentum_engine_route.momentum_engine_decision(
        cadence_hours=4,
        starting_capital=100.0,
        min_momentum_score=0.0,
        db="fake-db",
    )

    assert captured["db"] == "fake-db"
    assert captured["params"] == {"cadence_hours": 4, "starting_capital": 100.0, "min_momentum_score": 0.0}
    assert payload["source"] == "momentum_engine_status"
    assert payload["action"] == "BUY"
    assert payload["symbol"] == "ETHUSDC"
    assert payload["buy_symbol"] == "ETHUSDC"
    assert payload["sell_symbol"] is None
    assert payload["should_trade"] is True
    assert payload["executor_contract"]["action"] == "BUY"
    assert payload["executor_contract"]["order_sequence"] == [{"type": "BUY", "symbol": "ETHUSDC"}]
    assert payload["status"]["best_asset"] == {"symbol": "ETHUSDC", "rank": 1}

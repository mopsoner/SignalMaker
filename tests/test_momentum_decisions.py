from app.api.routes import momentum_engine as momentum_engine_route
from app.api.router import api_router


def test_momentum_engine_decision_route_is_registered() -> None:
    route = next(
        route
        for route in momentum_engine_route.router.routes
        if getattr(route, "path", None) == "/decision"
    )

    assert "GET" in route.methods


def test_removed_momentum_decision_history_page_has_no_api_route() -> None:
    paths = {getattr(route, "path", "") for route in momentum_engine_route.router.routes}

    assert "/decisions" not in paths


def test_momentum_candidates_route_is_not_registered() -> None:
    paths = {getattr(route, "path", "") for route in api_router.routes}

    assert "/api/v1/momentum-candidates" not in paths

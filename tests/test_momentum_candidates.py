from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.api.routes import momentum_candidates as momentum_candidates_route
from app.api.routes import momentum_engine as momentum_engine_route
from app.models.asset_state import AssetStateCurrent
from app.models.base import Base
from app.models.momentum_current import MomentumCurrent
from app.models.momentum_structure_current import MomentumStructureCurrent
from app.services.momentum_candidate_service import MomentumCandidateService


def _make_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    return Session(engine)


def _add_ready_momentum_asset(db: Session, *, symbol: str = "ETHUSDC", price: float = 100.0) -> None:
    db.add_all(
        [
            MomentumCurrent(
                symbol=symbol,
                price=price,
                momentum_score=12.0,
                classification="strong_bull",
                rsi_1h=50.0,
                rank=1,
                calculated_at=datetime.now(timezone.utc),
            ),
            MomentumStructureCurrent(
                symbol=symbol,
                structure_15m_status="valid",
                structure_15m_bias="neutral_bullish",
                last_swing_low_15m=96.0,
                last_swing_high_15m=104.0,
                structure_reason="15m_structure_holding_above_last_swing_low",
                calculated_at=datetime.now(timezone.utc),
            ),
        ]
    )


def test_momentum_candidates_route_is_registered() -> None:
    route = next(
        route
        for route in momentum_candidates_route.router.routes
        if getattr(route, "path", None) == ""
    )

    assert "GET" in route.methods


def test_momentum_candidate_builds_trade_candidate_from_ready_momentum_and_wyckoff_context() -> None:
    with _make_session() as db:
        _add_ready_momentum_asset(db)
        db.add(
            AssetStateCurrent(
                symbol="ETHUSDC",
                stage="trade",
                bias="bullish",
                score=88.0,
                price=100.0,
                liquidity_context={"type": "order_block", "level": 97.0},
                execution_target={"type": "range_high_1h", "level": 118.0},
                state_payload={
                    "symbol": "ETHUSDC",
                    "bias": "bullish",
                    "entry_liquidity_context": {"type": "bullish_order_block", "level": 97.0},
                    "external_swing_low": 96.0,
                    "internal_bull_pivot_low": 95.0,
                    "range_low_1h": 94.0,
                    "execution_target": {"type": "range_high_1h", "level": 118.0},
                    "projected_target": {"type": "range_high_4h", "level": 124.0},
                    "range_high_1h": 116.0,
                    "previous_day_high": 120.0,
                },
            )
        )
        db.commit()

        candidates = MomentumCandidateService(db).list_candidates(limit=10, min_rr=1.0)

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["candidate_id"] == "momentum-ETHUSDC-open"
    assert candidate["symbol"] == "ETHUSDC"
    assert candidate["side"] == "long"
    assert candidate["stage"] == "momentum_trade"
    assert candidate["status"] == "momentum_ready"
    assert candidate["entry_price"] == 100.0
    assert candidate["stop_price"] is not None
    assert candidate["stop_price"] < candidate["entry_price"]
    assert candidate["target_price"] > candidate["entry_price"]
    assert candidate["rr_ratio"] >= 1.0
    assert candidate["payload"]["momentum_asset"]["entry_status"] == "ready"
    assert candidate["payload"]["wyckoff_context_available"] is True


def test_momentum_candidates_skip_ready_asset_without_required_wyckoff_context() -> None:
    with _make_session() as db:
        _add_ready_momentum_asset(db)
        db.commit()

        strict_candidates = MomentumCandidateService(db).list_candidates(limit=10, min_rr=1.0)
        fallback_candidates = MomentumCandidateService(db).list_candidates(
            limit=10,
            min_rr=1.0,
            require_wyckoff_context=False,
        )

    assert strict_candidates == []
    assert fallback_candidates == []


def test_momentum_candidates_skip_assets_that_are_not_momentum_entry_ready() -> None:
    with _make_session() as db:
        _add_ready_momentum_asset(db)
        row = db.get(MomentumCurrent, "ETHUSDC")
        row.rsi_1h = 70.0
        db.add(
            AssetStateCurrent(
                symbol="ETHUSDC",
                stage="trade",
                state_payload={
                    "entry_liquidity_context": {"level": 97.0},
                    "external_swing_low": 96.0,
                    "internal_bull_pivot_low": 95.0,
                    "range_low_1h": 94.0,
                    "execution_target": {"level": 118.0},
                    "projected_target": {"level": 124.0},
                    "range_high_1h": 116.0,
                },
            )
        )
        db.commit()

        candidates = MomentumCandidateService(db).list_candidates(limit=10, min_rr=1.0)

    assert candidates == []


def test_momentum_engine_decision_route_is_removed() -> None:
    paths = {getattr(route, "path", None) for route in momentum_engine_route.router.routes}

    assert "/decision" not in paths

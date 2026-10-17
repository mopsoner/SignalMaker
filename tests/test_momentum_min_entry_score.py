import pytest
from sqlalchemy import select

from app.models.momentum_engine import MomentumEngineTrade
from app.services.momentum_engine_service import MomentumEngineService
from app.services.momentum_market import STOCK_ETF_CONTEXT
from test_momentum_engine_cash_balance import _make_session, _momentum_row, _open_engine_position


@pytest.mark.parametrize("score,eligible", [(19.99, False), (20, True), (20.01, True)])
@pytest.mark.parametrize("scenario", ["initial", "rotation", "risk_exit"])
def test_crypto_purchases_require_at_least_20_points(score, eligible, scenario):
    with _make_session() as db:
        db.add_all(_momentum_row("ETHUSDC", price=50, momentum_score=score, stored_rank=1))
        if scenario != "initial":
            db.add_all(_momentum_row("BTCUSDC", price=100, momentum_score=10, stored_rank=2,
                                    structure_15m_status="broken_bearish" if scenario == "risk_exit" else "valid"))
            db.add(_open_engine_position("BTCUSDC", entry_price=100, entry_value=1000, quantity=10, stored_rank=2))
        db.commit()
        service = MomentumEngineService(db)
        preview = service.decision(min_momentum_score=0)
        assert (preview["best_asset"] is not None) is eligible
        status = service.run_once(force=True, min_momentum_score=0)
        buys = list(db.scalars(select(MomentumEngineTrade).where(MomentumEngineTrade.action.like("BUY%"))))
        assert bool(buys) is eligible
        if eligible:
            assert service._open_position().symbol == "ETHUSDC"
        elif scenario == "risk_exit":
            assert service._open_position() is None
        elif scenario == "rotation":
            # Falling below 20 is an entry restriction, not an extra sale signal.
            assert service._open_position().symbol == "BTCUSDC"
        assert status["selection_min_momentum_score"] == 20


@pytest.mark.parametrize("score,minimum,eligible", [(20, -100, True), (19.99, -100, False), (24.99, 25, False), (25, 25, True)])
def test_runtime_minimum_cannot_lower_floor_but_can_raise_it(score, minimum, eligible):
    with _make_session() as db:
        service = MomentumEngineService(db)
        assert service._score_eligible({"momentum_score": score}, minimum) is eligible


def test_missing_score_is_blocked_and_stock_etf_threshold_is_preserved():
    with _make_session() as db:
        assert not MomentumEngineService(db)._score_eligible({}, 0)
        stock = MomentumEngineService(db, context=STOCK_ETF_CONTEXT)
        assert stock._score_eligible({"momentum_score": 10}, 0)
        assert not stock._score_eligible({"momentum_score": 10}, 10)

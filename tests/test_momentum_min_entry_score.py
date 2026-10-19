# Retain regression coverage of the explicitly supported one-slot engine.
from app.services.momentum_market import MomentumMarketContext
SINGLE_POSITION_CRYPTO = MomentumMarketContext(market_scope="crypto", reference_currency="USDC", max_positions=1)

import pytest
from sqlalchemy import select

from app.models.momentum_engine import MomentumEngineTrade
from app.services.momentum_engine_service import MomentumEngineService
from app.services.momentum_market import STOCK_ETF_CONTEXT
from test_momentum_engine_cash_balance import _make_session, _momentum_row, _open_engine_position


@pytest.mark.parametrize("classification,eligible", [("neutral_bull", False), ("bull", True), ("strong_bull", True), ("bear", False), (None, False)])
@pytest.mark.parametrize("scenario", ["initial", "rotation", "risk_exit"])
def test_crypto_purchases_require_bull_class(classification, eligible, scenario):
    with _make_session() as db:
        rows = _momentum_row("ETHUSDC", price=50, momentum_score=50, stored_rank=1)
        rows[0].classification = classification
        db.add_all(rows)
        if scenario != "initial":
            db.add_all(_momentum_row("BTCUSDC", price=100, momentum_score=5, stored_rank=2,
                                    structure_15m_status="broken_bearish" if scenario == "risk_exit" else "valid"))
            db.add(_open_engine_position("BTCUSDC", entry_price=100, entry_value=1000, quantity=10, stored_rank=2))
        db.commit()
        service = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)
        preview = service.decision(min_momentum_score=0)
        assert (preview["best_asset"] is not None) is (eligible and scenario != "rotation")
        status = service.run_once(force=True, min_momentum_score=0)
        buys = list(db.scalars(select(MomentumEngineTrade).where(MomentumEngineTrade.action.like("BUY%"))))
        assert bool(buys) is (eligible and scenario != "rotation")
        if eligible and scenario != "rotation":
            assert service._open_position().symbol == "ETHUSDC"
        elif scenario == "risk_exit":
            assert service._open_position() is None
        elif scenario == "rotation":
            # Entry eligibility never adds a sale signal to a held position.
            assert service._open_position().symbol == "BTCUSDC"
        assert status["selection_min_momentum_score"] is None
        assert status["selection_classes"] == ["bull", "strong_bull"]


@pytest.mark.parametrize("classification,score,minimum,eligible", [
    ("bull", 1, 100, True), ("strong_bull", 1, 100, True),
    ("neutral_bull", 100, 0, False), ("bear", 100, -100, False),
    (None, 100, 0, False), ("unknown", 100, 0, False),
])
def test_crypto_uses_classification_instead_of_numeric_threshold(classification, score, minimum, eligible):
    with _make_session() as db:
        assert MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)._entry_eligible(
            {"classification": classification, "momentum_score": score, "momentum_15m": 16.0, "momentum_acceleration": 6.0}, minimum) is eligible


def test_missing_class_is_blocked_and_stock_etf_threshold_is_preserved():
    with _make_session() as db:
        assert not MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)._entry_eligible({}, 0)
        stock = MomentumEngineService(db, context=STOCK_ETF_CONTEXT)
        assert stock._entry_eligible({"momentum_score": 10}, 0)
        assert not stock._entry_eligible({"momentum_score": 10}, 10)


def test_crypto_holds_until_exit_then_selects_next_eligible_rank():
    with _make_session() as db:
        for rank, symbol, score, structure in [
            (1, "BTCUSDC", 40, "valid"),
            (2, "ETHUSDC", 30, "valid"),
            (3, "SOLUSDC", 25, "broken_bearish"),
            (4, "BNBUSDC", 10, "valid"),
            (5, "XRPUSDC", 9.99, "valid"),
        ]:
            rows = _momentum_row(symbol, price=50, momentum_score=score,
                                 stored_rank=rank, structure_15m_status=structure)
            rows[0].classification = "neutral_bull" if symbol == "XRPUSDC" else "bull"
            db.add_all(rows)
        db.commit()
        service = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)
        for expected in ["BNBUSDC", "ETHUSDC", "BTCUSDC"]:
            preview = service.decision(min_momentum_score=10)
            assert preview["best_asset"]["symbol"] == expected
            service.run_once(force=True, min_momentum_score=10)
            assert service._open_position().symbol == expected
            for _ in range(2):
                hold = service.decision(min_momentum_score=10)
                assert hold["best_asset"] is None
                assert hold["action"] == "hold"
                assert hold["target_symbol"] == expected
                service.run_once(force=True, min_momentum_score=10)
                assert service._open_position().symbol == expected
            if expected != "BTCUSDC":
                from app.models.momentum_structure_current import MomentumStructureCurrent
                structure = db.scalar(select(MomentumStructureCurrent).where(
                    MomentumStructureCurrent.symbol == expected))
                structure.structure_15m_status = "broken_bearish"
                db.commit()
        assert service.decision(min_momentum_score=10)["best_asset"] is None
        service.run_once(force=True, min_momentum_score=10)
        assert service._open_position().symbol == "BTCUSDC"
        buys = list(db.scalars(select(MomentumEngineTrade).where(
            MomentumEngineTrade.action.like("BUY%"))))
        assert [trade.symbol for trade in buys] == ["BNBUSDC", "ETHUSDC", "BTCUSDC"]


@pytest.mark.parametrize("starting_capital", [1000, 0])
def test_cash_entry_bypasses_cadence_only_when_funded_and_eligible(starting_capital):
    from app.models.momentum_current import MomentumCurrent
    with _make_session() as db:
        rows = _momentum_row("ETHUSDC", price=50, momentum_score=15, stored_rank=1)
        rows[0].classification = "neutral_bull"
        db.add_all(rows)
        db.commit()
        service = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)
        service.run_once(force=True, starting_capital=starting_capital)
        assert service.status(starting_capital=starting_capital)["due_now"] is False
        db.scalar(select(MomentumCurrent)).classification = "bull"
        db.commit()
        preview = service.decision(starting_capital=starting_capital)
        assert preview["cadence_due"] is False
        assert preview["event_due"] is bool(starting_capital)
        assert preview["due_now"] is bool(starting_capital)
        before_count = len(list(db.scalars(select(MomentumEngineTrade))))
        result = service.run_once(starting_capital=starting_capital)
        if starting_capital:
            assert preview["action"] == "buy"
            assert result["due_reason"] == "eligible_while_cash"
            assert service._open_position().symbol == "ETHUSDC"
            assert len(list(db.scalars(select(MomentumEngineTrade)))) == before_count + 1
            service.run_once(starting_capital=starting_capital)
            assert len(list(db.scalars(select(MomentumEngineTrade)))) == before_count + 1
        else:
            assert service._open_position() is None
            assert len(list(db.scalars(select(MomentumEngineTrade)))) == before_count

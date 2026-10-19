# Retain regression coverage of the explicitly supported one-slot engine.
from app.services.momentum_market import MomentumMarketContext
SINGLE_POSITION_CRYPTO = MomentumMarketContext(market_scope="crypto", reference_currency="USDC", max_positions=1)

import pytest
from sqlalchemy import select

from app.core.config import settings
from app.models.momentum_engine import MomentumEngineTrade
from app.services.momentum_engine_service import MomentumEngineService
from test_momentum_engine_cash_balance import _make_session, _momentum_row, _open_engine_position


PREALERT = {'momentum_delta_15m': 1, 'momentum_delta_1h': 1, 'momentum_acceleration_1h': 1}


@pytest.mark.parametrize('field', list(PREALERT))
@pytest.mark.parametrize('value,eligible', [
    (1, True), (0.00001, True), ('1.5', True), (0, False), (-1, False),
    (None, False), ('', False), (' ', False), (True, False), (False, False),
    (float('nan'), False), (float('inf'), False), ('invalid', False),
])
def test_prealert_strictly_positive_finite_inputs(field, value, eligible):
    with _make_session() as db:
        row = {'classification': 'bull', **PREALERT, field: value}
        service = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)
        assert service._entry_eligible(row, 0) is eligible
        del row[field]
        assert not service._entry_eligible(row, 0)


def test_obsolete_entry_thresholds_no_longer_apply(monkeypatch):
    monkeypatch.setattr(settings, 'momentum_entry_min_15m', 100)
    monkeypatch.setattr(settings, 'momentum_entry_min_acceleration', 100)
    with _make_session() as db:
        service = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)
        row = {'classification': 'bull', **PREALERT, 'momentum_15m': -1, 'momentum_acceleration': -5}
        assert service._entry_eligible(row, 100)
        assert service._selection_metadata()['selection_min_acceleration'] is None


@pytest.mark.parametrize('scenario', ['cash', 'hold', 'risk_exit'])
def test_filter_blocks_buys_without_forcing_a_sale(scenario):
    with _make_session() as db:
        candidate = _momentum_row('ETHUSDC', price=50, momentum_score=30, stored_rank=2)
        candidate[0].momentum_delta_15m = 0
        db.add_all(candidate)
        if scenario != 'cash':
            db.add_all(_momentum_row('BTCUSDC', price=100, momentum_score=40, stored_rank=1,
                structure_15m_status='broken_bearish' if scenario == 'risk_exit' else 'valid'))
            db.add(_open_engine_position('BTCUSDC', entry_price=100, entry_value=1000, quantity=10, stored_rank=1))
        else:
            candidate[0].momentum_15m = -1
        db.commit()
        service = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)
        if scenario == 'cash':
            assert service.decision()['best_asset'] is None
        service.run_once(force=True)
        trades = list(db.scalars(select(MomentumEngineTrade)))
        assert not [t for t in trades if t.action.startswith('BUY')]
        assert bool(service._open_position()) is (scenario == 'hold')


def test_lost_prealert_does_not_exit_existing_position():
    with _make_session() as db:
        rows = _momentum_row('BTCUSDC', price=100, momentum_score=40, stored_rank=2)
        rows[0].momentum_acceleration_1h = 0
        db.add_all(rows)
        db.add(_open_engine_position('BTCUSDC', entry_price=100, entry_value=1000, quantity=10, stored_rank=1))
        db.commit()
        service = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)
        service.run_once(force=True)
        assert service._open_position().symbol == 'BTCUSDC'
        assert not list(db.scalars(select(MomentumEngineTrade).where(MomentumEngineTrade.action.like('SELL%'))))


def test_selection_skips_missing_prealert_for_next_valid_asset():
    with _make_session() as db:
        weak = _momentum_row('ETHUSDC', price=50, momentum_score=50, stored_rank=1)
        weak[0].momentum_delta_1h = 0
        db.add_all(weak)
        db.add_all(_momentum_row('BTCUSDC', price=100, momentum_score=40, stored_rank=2))
        db.commit()
        service = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)
        assert service.decision()['best_asset']['symbol'] == 'BTCUSDC'
        service.run_once(force=True)
        assert service._open_position().symbol == 'BTCUSDC'


@pytest.mark.parametrize('price', [0, -1, None, 'invalid', float('inf'), float('nan')])
def test_invalid_prices_are_never_selected(price):
    with _make_session() as db:
        row = {'symbol': 'BTCUSDC', 'price': price, 'classification': 'bull', **PREALERT}
        service = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)
        assert service._best_ranked_asset_with_valid_structure(rankings=[row], min_momentum_score=0) is None


def test_crypto_orders_by_score_then_rank_with_deterministic_ties():
    with _make_session() as db:
        service = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)
        rows = [{'symbol': 'A', 'momentum_score': 2, 'rank': 1},
                {'symbol': 'B', 'momentum_score': 5, 'rank': 3},
                {'symbol': 'C', 'momentum_score': 5, 'rank': 2},
                {'symbol': 'D', 'momentum_score': None, 'rank': 4}]
        assert [r['symbol'] for r in service._entry_order(rows)] == ['C', 'B', 'A', 'D']

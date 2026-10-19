import pytest
from sqlalchemy import select

from app.core.config import settings
from app.models.momentum_current import MomentumCurrent
from app.models.momentum_engine import MomentumEngineTrade
from app.services.momentum_engine_service import MomentumEngineService
from test_momentum_engine_cash_balance import _make_session, _momentum_row, _open_engine_position


@pytest.mark.parametrize('momentum,acceleration,eligible', [
    (1, 6, True), (0, 6, False), (-1, 6, False), (1, 5, False), (1, -6, False),
    (None, 6, False), (1, None, False), (float('nan'), 6, False),
    (1, float('inf'), False), ('invalid', 6, False),
])
def test_strict_entry_thresholds_and_missing_invalid_data(monkeypatch, momentum, acceleration, eligible):
    monkeypatch.setattr(settings, 'momentum_entry_min_15m', 0)
    monkeypatch.setattr(settings, 'momentum_entry_min_acceleration', 5)
    with _make_session() as db:
        assert MomentumEngineService(db)._entry_eligible({
            'classification': 'bull', 'momentum_15m': momentum,
            'momentum_acceleration': acceleration}, 0) is eligible


def test_configured_threshold_is_used(monkeypatch):
    monkeypatch.setattr(settings, 'momentum_entry_min_15m', 2)
    with _make_session() as db:
        service = MomentumEngineService(db)
        row = {'classification': 'bull', 'momentum_15m': 2, 'momentum_acceleration': 6}
        assert not service._entry_eligible(row, 0)
        row['momentum_15m'] = 2.01
        assert service._entry_eligible(row, 0)


@pytest.mark.parametrize('scenario', ['cash', 'hold', 'risk_exit'])
def test_filter_blocks_buys_without_forcing_a_sale(scenario):
    with _make_session() as db:
        candidate = _momentum_row('ETHUSDC', price=50, momentum_score=30, stored_rank=2)
        candidate[0].momentum_acceleration = 5
        db.add_all(candidate)
        if scenario != 'cash':
            db.add_all(_momentum_row('BTCUSDC', price=100, momentum_score=40, stored_rank=1,
                structure_15m_status='broken_bearish' if scenario == 'risk_exit' else 'valid'))
            db.add(_open_engine_position('BTCUSDC', entry_price=100, entry_value=1000, quantity=10, stored_rank=1))
        else:
            candidate[0].momentum_15m = -1
        db.commit()
        service = MomentumEngineService(db)
        if scenario == 'cash':
            assert service.decision()['best_asset'] is None
        service.run_once(force=True)
        trades = list(db.scalars(select(MomentumEngineTrade)))
        assert not [t for t in trades if t.action.startswith('BUY')]
        assert bool(service._open_position()) is (scenario == 'hold')


def test_lost_entry_momentum_does_not_exit_existing_position():
    with _make_session() as db:
        rows = _momentum_row('BTCUSDC', price=100, momentum_score=40, stored_rank=1)
        rows[0].momentum_acceleration = 0
        db.add_all(rows)
        db.add(_open_engine_position('BTCUSDC', entry_price=100, entry_value=1000, quantity=10, stored_rank=1))
        db.commit()
        service = MomentumEngineService(db)
        service.run_once(force=True)
        assert service._open_position().symbol == 'BTCUSDC'
        assert not list(db.scalars(select(MomentumEngineTrade).where(MomentumEngineTrade.action.like('SELL%'))))


def test_selection_skips_weak_acceleration_for_next_valid_asset():
    with _make_session() as db:
        weak = _momentum_row('ETHUSDC', price=50, momentum_score=30, stored_rank=2)
        weak[0].momentum_acceleration = 5
        db.add_all(weak)
        db.add_all(_momentum_row('BTCUSDC', price=100, momentum_score=40, stored_rank=1))
        db.commit()
        service = MomentumEngineService(db)
        assert service.decision()['best_asset']['symbol'] == 'BTCUSDC'
        service.run_once(force=True)
        assert service._open_position().symbol == 'BTCUSDC'


def test_global_acceleration_is_used_not_15m_acceleration():
    with _make_session() as db:
        assert not MomentumEngineService(db)._entry_eligible({
            'classification': 'bull', 'momentum_15m': 16,
            'momentum_acceleration': 4, 'momentum_acceleration_15m': 20}, 0)


@pytest.mark.parametrize('momentum,eligible', [(14.99, False), (15, False), (15.01, True)])
def test_default_15m_threshold_boundary(monkeypatch, momentum, eligible):
    from app.core.config import Settings
    assert Settings.model_fields['momentum_entry_min_15m'].default == 15
    assert Settings.model_fields['momentum_entry_min_acceleration'].default == 5
    monkeypatch.setattr(settings, 'momentum_entry_min_15m', 15)
    monkeypatch.setattr(settings, 'momentum_entry_min_acceleration', 5)
    with _make_session() as db:
        assert MomentumEngineService(db)._entry_eligible({
            'classification': 'bull', 'momentum_15m': momentum,
            'momentum_acceleration': 6}, 0) is eligible

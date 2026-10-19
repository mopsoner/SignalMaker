# Retain regression coverage of the explicitly supported one-slot engine.
from app.services.momentum_market import MomentumMarketContext
SINGLE_POSITION_CRYPTO = MomentumMarketContext(market_scope="crypto", reference_currency="USDC", max_positions=1)

import pytest
from sqlalchemy import select

from app.core.config import settings
from app.models.momentum_engine import MomentumEngineTrade
from app.services.momentum_engine_service import MomentumEngineService
from app.services.momentum_market import STOCK_ETF_CONTEXT
from test_momentum_engine_cash_balance import _make_session, _momentum_row, _open_engine_position
from test_momentum_rsi_and_15m_exit import add_candles


@pytest.mark.parametrize('momentum,exit_expected', [
    (1, False), (0.01, False), (0, True), (-1, True),
    (None, True), ('invalid', True), (float('nan'), True), (float('inf'), True),
])
def test_hourly_confirmation_and_missing_data_fallback(monkeypatch, momentum, exit_expected):
    monkeypatch.setattr(settings, 'momentum_exit_max_1h', 0)
    with _make_session() as db:
        service = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)
        asset = {'closed_change_15m': -1, 'momentum_1h': momentum}
        assert service._negative_15m_exit(asset) is exit_expected
        assert service._entry_valid(asset) is (not exit_expected)
        asset['closed_change_15m'] = 0
        assert not service._negative_15m_exit(asset)


@pytest.mark.parametrize('protection', ['support', 'ema', 'none'])
def test_other_exit_protections_remain_independent(protection):
    with _make_session() as db:
        service = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)
        asset = {'closed_change_15m': -1, 'momentum_1h': 20,
                 'structure_15m_status': 'broken_bearish' if protection == 'support' else 'valid',
                 'ema_trend_15m': 'below_ema' if protection == 'ema' else 'above_ema'}
        assert service._exit_required(asset) is (protection != 'none')


@pytest.mark.parametrize('momentum,sell', [(2, False), (0, True), (-2, True), (None, True)])
def test_closed_candle_drop_holds_or_exits_in_actual_engine(momentum, sell):
    with _make_session() as db:
        rows = _momentum_row('BTCUSDC', price=100, momentum_score=30, stored_rank=1)
        rows[0].momentum_1h = momentum
        db.add_all(rows)
        db.add(_open_engine_position('BTCUSDC', entry_price=100, entry_value=1000, quantity=10, stored_rank=1))
        add_candles(db, '15m', [110, 100])
        db.commit()
        service = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)
        decision = service.decision()
        assert decision['action'] == ('sell' if sell else 'hold')
        service.run_once(force=True)
        sales = list(db.scalars(select(MomentumEngineTrade).where(MomentumEngineTrade.action.like('SELL%'))))
        assert bool(sales) is sell
        assert bool(service._open_position()) is not sell


def test_entry_veto_matches_exit_confirmation_and_preserves_stock_etf(monkeypatch):
    monkeypatch.setattr(settings, 'momentum_exit_max_1h', 3)
    with _make_session() as db:
        service = MomentumEngineService(db, context=SINGLE_POSITION_CRYPTO)
        asset = {'price': 100, 'classification': 'bull', 'momentum_delta_15m': 1, 'momentum_delta_1h': 1, 'momentum_acceleration_1h': 1, 'closed_change_15m': -1, 'momentum_1h': 20, 'structure_15m_status': 'valid', 'ema_trend_15m': 'above_ema'}
        assert service._entry_valid(asset)
        assert service._support_status(asset) == 'prealert_valid'
        asset['momentum_1h'] = 3
        assert service._negative_15m_exit(asset)
        assert not service._entry_valid(asset)
        assert not MomentumEngineService(db, context=STOCK_ETF_CONTEXT)._negative_15m_exit(asset)

import pytest
from sqlalchemy import select
from app.models.momentum_current import MomentumCurrent
from app.models.momentum_structure_current import MomentumStructureCurrent
from app.models.momentum_engine import MomentumEngineTrade
from app.schemas.momentum_engine import MomentumEngineStatus, MomentumEngineDecision
from app.services.momentum_engine_service import MomentumEngineService
from test_momentum_engine_cash_balance import _make_session, _momentum_row


def seed(db, symbols=('BTCUSDC', 'ETHUSDC', 'SOLUSDC', 'XRPUSDC')):
    for index, symbol in enumerate(symbols):
        db.add_all(_momentum_row(symbol, price=100, momentum_score=40-index, stored_rank=index+1))
    db.commit()


def test_opens_three_distinct_equal_positions_and_reports_total_equity():
    with _make_session() as db:
        seed(db)
        engine = MomentumEngineService(db)
        preview = engine.decision(starting_capital=900)
        assert len(preview['operations']) == 3
        assert [op['symbol'] for op in preview['operations']] == ['BTCUSDC', 'ETHUSDC', 'SOLUSDC']
        assert MomentumEngineDecision.model_validate(preview).selection_prealert_fields == ['momentum_delta_15m', 'momentum_delta_1h', 'momentum_acceleration_1h']
        result = engine.run_once(starting_capital=900)
        assert len(result['open_positions']) == 3
        assert len({p['symbol'] for p in result['open_positions']}) == 3
        assert [p.entry_value for p in engine._open_positions()] == pytest.approx([300]*3)
        assert result['cash'] == pytest.approx(0)
        assert result['equity'] == pytest.approx(900)
        assert len(MomentumEngineStatus.model_validate(result).open_positions) == 3
        decision = engine.current_decision()
        assert len(MomentumEngineDecision.model_validate(decision).operations) == 3
        assert [op['allocation_divisor'] for op in decision['operations']] == [3, 2, 1]
        engine.run_once(starting_capital=900)
        assert len(list(db.scalars(select(MomentumEngineTrade).where(MomentumEngineTrade.action.like('BUY%'))))) == 3


def test_preserves_cash_for_empty_slots_then_fills_them_without_selling():
    with _make_session() as db:
        seed(db, ('BTCUSDC',))
        engine = MomentumEngineService(db)
        result = engine.run_once(starting_capital=900)
        assert result['open_positions'][0]['entry_value'] == pytest.approx(300)
        assert result['cash'] == pytest.approx(600)
        seed(db, ('ETHUSDC', 'SOLUSDC'))
        assert engine.status(starting_capital=900)['event_due']
        result = engine.run_once(starting_capital=900)
        assert len(result['open_positions']) == 3
        assert all(p['entry_value'] == pytest.approx(300) for p in result['open_positions'])
        assert not list(db.scalars(select(MomentumEngineTrade).where(MomentumEngineTrade.action.like('SELL%'))))


def test_exits_each_position_independently_and_replaces_only_broken_one():
    with _make_session() as db:
        seed(db)
        engine = MomentumEngineService(db)
        engine.run_once(starting_capital=900)
        held = engine._open_positions()
        broken = held[1].symbol
        retained = {p.position_id for p in held if p.symbol != broken}
        structure = db.scalar(select(MomentumStructureCurrent).where(MomentumStructureCurrent.symbol == broken))
        structure.structure_15m_status = 'broken_bearish'
        db.commit()
        result = engine.run_once(starting_capital=900)
        assert len(result['open_positions']) == 3
        assert retained.issubset({p['position_id'] for p in result['open_positions']})
        assert broken not in {p['symbol'] for p in result['open_positions']}
        intent = engine.current_decision()
        assert [op['action'] for op in intent['operations']] == ['sell', 'buy']
        assert intent['operations'][1]['allocation_divisor'] == 1


def test_marks_all_positions_and_holds_full_portfolio():
    with _make_session() as db:
        seed(db)
        engine = MomentumEngineService(db)
        engine.run_once(starting_capital=900)
        for p in engine._open_positions():
            db.scalar(select(MomentumCurrent).where(MomentumCurrent.symbol == p.symbol)).price = 110
        db.commit()
        result = engine.run_once(force=True, starting_capital=900)
        assert result['equity'] == pytest.approx(990)
        assert sum(p['unrealized_pnl'] for p in result['open_positions']) == pytest.approx(90)
        assert engine.current_decision()['should_trade'] is False


def test_simultaneous_exits_keep_cash_and_do_not_rebuy_sold_assets():
    with _make_session() as db:
        seed(db, ('BTCUSDC', 'ETHUSDC', 'SOLUSDC'))
        engine = MomentumEngineService(db)
        engine.run_once(starting_capital=900)
        for row in db.scalars(select(MomentumStructureCurrent)):
            row.structure_15m_status = 'broken_bearish'
        db.commit()
        result = engine.run_once(starting_capital=900)
        assert result['open_positions'] == []
        assert result['cash'] == pytest.approx(900)
        assert len(engine.current_decision()['operations']) == 3


def test_portfolio_filters_reject_weak_entry_and_hourly_confirmation_holds_independently():
    from test_momentum_rsi_and_15m_exit import add_candles
    with _make_session() as db:
        seed(db, ('BTCUSDC', 'ETHUSDC', 'SOLUSDC'))
        weak = db.scalar(select(MomentumCurrent).where(MomentumCurrent.symbol == 'SOLUSDC'))
        weak.momentum_delta_15m = 0
        strong = db.scalar(select(MomentumCurrent).where(MomentumCurrent.symbol == 'BTCUSDC'))
        strong.momentum_1h = 10
        db.commit()
        engine = MomentumEngineService(db)
        engine.run_once(starting_capital=900)
        assert {p.symbol for p in engine._open_positions()} == {'BTCUSDC', 'ETHUSDC'}
        add_candles(db, '15m', [110, 100])
        db.commit()
        engine.run_once(force=True, starting_capital=900)
        assert {p.symbol for p in engine._open_positions()} == {'BTCUSDC', 'ETHUSDC'}
        strong.momentum_1h = -1
        db.commit()
        engine.run_once(starting_capital=900)
        assert {p.symbol for p in engine._open_positions()} == {'ETHUSDC'}


@pytest.mark.parametrize('capacity', [1, 2, 4])
def test_env_capacity_controls_number_and_allocation(monkeypatch, capacity):
    from app.core.config import settings
    monkeypatch.setattr(settings, 'momentum_max_positions', capacity)
    with _make_session() as db:
        seed(db)
        result = MomentumEngineService(db).run_once(starting_capital=1200)
        assert result['max_positions'] == capacity
        assert len(result['open_positions']) == capacity
        assert [p['entry_value'] for p in result['open_positions']] == pytest.approx([1200/capacity]*capacity)


def test_reducing_capacity_keeps_all_holdings_monitored_without_new_entries(monkeypatch):
    from app.core.config import settings
    with _make_session() as db:
        seed(db)
        MomentumEngineService(db).run_once(starting_capital=900)
        monkeypatch.setattr(settings, 'momentum_max_positions', 1)
        engine = MomentumEngineService(db)
        result = engine.run_once(force=True, starting_capital=900)
        assert len(result['open_positions']) == 3
        assert not engine.current_decision()['operations']
        for row in db.scalars(select(MomentumStructureCurrent)):
            row.structure_15m_status = 'broken_bearish'
        db.commit()
        result = engine.run_once(starting_capital=900)
        assert result['open_positions'] == []
        assert len(engine.current_decision()['operations']) == 3


@pytest.mark.parametrize('capacity', [3, 4, 5])
def test_default_initial_cash_is_1000_regardless_of_capacity(monkeypatch, capacity):
    from app.core.config import settings
    monkeypatch.setattr(settings, 'momentum_max_positions', capacity)
    with _make_session() as db:
        seed(db, ('BTCUSDC', 'ETHUSDC', 'SOLUSDC', 'XRPUSDC', 'BNBUSDC'))
        result = MomentumEngineService(db).run_once()
        assert result['starting_capital'] == 1000
        assert result['equity'] == pytest.approx(1000)
        assert len(result['open_positions']) == capacity
        assert [p['entry_value'] for p in result['open_positions']] == pytest.approx([1000/capacity]*capacity)
        assert result['cash'] == pytest.approx(0)


def test_full_portfolio_keeps_holdings_after_prealert_loss_and_better_candidate():
    with _make_session() as db:
        seed(db, ('BTCUSDC', 'ETHUSDC', 'SOLUSDC'))
        engine = MomentumEngineService(db)
        engine.run_once(starting_capital=900)
        held_ids = {p.position_id for p in engine._open_positions()}
        for row in db.scalars(select(MomentumCurrent)):
            row.momentum_delta_15m = -1
            row.classification = 'neutral_bull'
            row.momentum_score = 1
        db.add_all(_momentum_row('XRPUSDC', price=100, momentum_score=100, stored_rank=1))
        db.commit()
        result = engine.run_once(force=True, starting_capital=900)
        assert {p['position_id'] for p in result['open_positions']} == held_ids
        assert engine.current_decision()['operations'] == []
        assert not list(db.scalars(select(MomentumEngineTrade).where(MomentumEngineTrade.action.like('SELL%'))))


@pytest.mark.parametrize('exit_field,exit_value', [
    ('structure_15m_status', 'broken_bearish'),
    ('mss_15m_bearish', True), ('bos_15m_bearish', True),
    ('ema_trend_15m', 'below_ema'),
])
def test_prealert_cannot_override_active_exit(exit_field, exit_value):
    with _make_session() as db:
        engine = MomentumEngineService(db)
        eligible = {'symbol': 'BTCUSDC', 'price': 100, 'momentum_score': 100,
                    'classification': 'strong_bull', 'momentum_delta_15m': 1,
                    'momentum_delta_1h': 1, 'momentum_acceleration_1h': 1,
                    exit_field: exit_value}
        assert engine._best_ranked_asset_with_valid_structure(rankings=[eligible], min_momentum_score=0) is None

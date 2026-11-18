from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models.base import Base
from app.models.position import Position
from app.services.kraken_profit_history import daily_profit, record_profit
from app.services.wyckoff_goal_service import WyckoffGoalService


def test_daily_profit_separates_currencies_and_symbols_and_uses_paris_day():
    with Session(create_engine('sqlite://')) as db:
        Base.metadata.create_all(db.get_bind())
        def add(key, symbol, value, at, **kwargs):
            row = Position(position_id=key, symbol=symbol, side='long', status=kwargs.get('status', 'closed'),
                closed_at=datetime.fromisoformat(at), quantity=1,
                meta={'execution_mode': kwargs.get('mode', 'live'), 'realized_profit': {
                    'verified': kwargs.get('verified', True), 'value': value,
                    'currency': 'USDC' if symbol.endswith('USDC') else 'USD'}})
            db.add(row)
            if row.status == 'closed':
                record_profit(db, row)
                record_profit(db, row)  # Repeated close confirmation must not double count.
        add('a', 'BTCUSD', 10, '2026-11-17T23:30:00+00:00')  # Nov 18 in Paris
        add('b', 'BTCUSD', -4, '2026-11-18T01:00:00+00:00')
        add('c', 'ETHUSD', -3, '2026-11-17T12:00:00+00:00')
        add('d', 'BTCUSDC', 8, '2026-11-18T01:00:00+00:00')
        add('paper', 'BTCUSD', 999, '2026-11-18T01:00:00+00:00', mode='paper')
        add('open', 'BTCUSD', 999, '2026-11-18T01:00:00+00:00', status='open')
        add('unknown', 'BTCUSD', 99, '2026-11-18T01:00:00+00:00', verified=False)
        add('bad', 'BTCUSD', float('inf'), '2026-11-18T01:00:00+00:00')
        add('old', 'BTCUSD', 999, '2026-10-01T01:00:00+00:00')
        db.commit()
        result = daily_profit(db, days=7, now=datetime(2026, 11, 18, 12, tzinfo=timezone.utc))
        assert result['dates'] == [f'2026-11-{day}' for day in range(12, 19)]
        series = {(s['symbol'], s['currency']): s['values'] for s in result['series']}
        assert series['BTCUSD', 'USD'] == [0, 0, 0, 0, 0, 0, 6]
        assert series['ETHUSD', 'USD'] == [0, 0, 0, 0, 0, -3, 0]
        assert series['BTCUSDC', 'USDC'][-1] == 8
        assert result['closedCount'] == 4
        assert len(result['series']) == 3


@pytest.mark.parametrize('side, expected', [('long', 20), ('short', -20)])
def test_confirmed_goal_fill_records_actual_close_profit(side, expected):
    with Session(create_engine('sqlite://')) as db:
        Base.metadata.create_all(db.get_bind())
        row = Position(position_id='p', symbol='BTCUSD', side=side, status='closing',
            entry_price=100, quantity=2, unrealized_pnl=999,
            meta={'execution_mode': 'live', 'goal_close_order_id': 'exit'})
        db.add(row)
        db.commit()
        ex = SimpleNamespace(get_order=lambda *a, **kw: {'status': 'filled', 'average_price': 110})
        WyckoffGoalService(db, execution=ex)._close(row)
        assert row.status == 'closed'
        assert row.meta['realized_profit']['value'] == expected
        assert row.meta['realized_profit']['verified'] is True


def test_partial_or_invalid_closes_are_not_marked_verified():
    service = WyckoffGoalService(None)
    row = SimpleNamespace(meta={'goal_closed_quantity': 1}, entry_price=100, symbol='BTCUSD', side='long')
    service._record_realized_profit(row, {'average_price': 110}, 2)
    assert 'realized_profit' not in row.meta
    row.meta = {}
    service._record_realized_profit(row, {'average_price': float('nan')}, 2)
    assert 'realized_profit' not in row.meta


def test_profit_reads_only_daily_totals_and_rollback_is_atomic():
    from sqlalchemy import event
    with Session(create_engine('sqlite://')) as db:
        Base.metadata.create_all(db.get_bind())
        row = Position(position_id='rolled-back', symbol='ETHUSD', side='long', status='closed',
            quantity=1, closed_at=datetime.now(timezone.utc), meta={'execution_mode': 'live',
            'realized_profit': {'verified': True, 'value': 4, 'currency': 'USD'}})
        db.add(row)
        record_profit(db, row)
        db.rollback()
        queries = []
        event.listen(db.get_bind(), 'before_cursor_execute',
            lambda conn, cursor, statement, parameters, context, many: queries.append(statement))
        result = daily_profit(db)
        assert result['series'] == []
        assert len(queries) == 1
        assert 'kraken_daily_profits' in queries[0]
        assert 'FROM positions' not in queries[0]


def test_profit_migration_is_idempotent():
    import sqlite3
    from pathlib import Path
    sql = Path('migrations/20261118_add_kraken_daily_profits.sql').read_text()
    with sqlite3.connect(':memory:') as db:
        db.executescript(sql)
        db.execute("INSERT INTO kraken_daily_profits VALUES ('2026-11-18','BTCUSD','USD',12,1)")
        db.executescript(sql)
        assert db.execute('SELECT value, closed_count FROM kraken_daily_profits').fetchall() == [(12, 1)]


def test_full_spot_take_profit_updates_total_once():
    with Session(create_engine('sqlite://')) as db:
        Base.metadata.create_all(db.get_bind())
        row = Position(position_id='spot-tp', symbol='BTCUSD', side='long', status='open',
            entry_price=100, quantity=2, entry_order_id='entry', take_profit_order_id='tp',
            meta={'execution_mode': 'live', 'kraken_mode': 'spot'})
        db.add(row)
        db.commit()
        ex = SimpleNamespace(rules=SimpleNamespace(quote_asset=lambda _: 'USD'),
            get_order=lambda symbol, order_id, **kw: {'status': 'filled',
                'executed_quantity': 2, 'average_price': 100 if order_id == 'entry' else 110})
        service = WyckoffGoalService(db, execution=ex)
        service._scan(row, {})
        service._scan(row, {})
        result = daily_profit(db)
        assert result['closedCount'] == 1
        assert sum(result['series'][0]['values']) == 20

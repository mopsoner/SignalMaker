from unittest.mock import Mock, patch
from datetime import datetime, timezone

from fastapi import HTTPException
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models.base import Base
from app.models.momentum_engine import MomentumEnginePosition
from app.models.market_candle import MarketCandle
from app.services.momentum_ai_service import MomentumAIService


def test_snapshot_is_scoped_and_excludes_future_candles():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add_all([MomentumEnginePosition(position_id='crypto', symbol='BTCUSD', entry_price=10, entry_value=10),
                    MomentumEnginePosition(position_id='stock', symbol='AAPL', market_scope='stock_etf', entry_price=10, entry_value=10)])
        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        for name, stamp in [('past', now-100000), ('future', now+100000)]:
            db.add(MarketCandle(candle_id=name, symbol='BTCUSD', interval='1h', open_time=stamp-3600000,
                                close_time=stamp, open=10, high=11, low=9, close=10, volume=1))
        db.commit()
        snapshot = MomentumAIService(db).snapshot()
        assert [p['position_id'] for p in snapshot['positions']] == ['crypto']
        assert snapshot['entry_filters'] == {
            'classes': ['bull', 'strong_bull'],
            'prealert_strictly_above_zero': ['momentum_delta_15m', 'momentum_delta_1h', 'momentum_acceleration_1h'],
            'order': 'momentum_score_desc', 'veto_active_exit': True,
        }
        assert len(snapshot['price_history']['BTCUSD']['1h']) == 1
        assert snapshot['price_history']['BTCUSD']['1h'][0]['close_time'] < now
        assert not db.new and not db.dirty and not db.deleted


def test_missing_key_does_not_call_provider():
    with patch('app.services.momentum_ai_service.settings') as config, patch('app.services.momentum_ai_service.requests.post') as post:
        config.openai_api_key = ''
        with pytest.raises(HTTPException) as error:
            MomentumAIService(Mock()).analyze()
        assert error.value.status_code == 503
        post.assert_not_called()


def test_completed_report_and_provider_errors():
    service = MomentumAIService(Mock())
    service.snapshot = Mock(return_value={'positions': [1], 'ranking': [], 'trades': [], 'as_of': datetime.now(timezone.utc).isoformat(), 'limits': {}, 'truncated': {}})
    with patch('app.services.momentum_ai_service.settings') as config, patch('app.services.momentum_ai_service.requests.post') as post:
        config.openai_api_key = 'test-key'
        config.openai_model = 'test-model'
        post.return_value.json.return_value = {'status': 'completed', 'output': [{'type': 'message', 'content': [{'type': 'output_text', 'text': 'Analyse globale'}]}]}
        assert service.analyze()['report'] == 'Analyse globale'
        assert post.call_args.kwargs['json']['model'] == 'test-model'
        post.return_value.json.return_value['status'] = 'incomplete'
        with pytest.raises(HTTPException) as error:
            service.analyze()
        assert error.value.status_code == 502


def test_preview_counts_missing_and_losing_positions_without_provider_call():
    service = MomentumAIService(Mock())
    service.snapshot = Mock(return_value={
        'as_of': datetime.now(timezone.utc).isoformat(), 'history_since': 'then',
        'positions': [{'status': 'open', 'unrealized_pnl': -2}, {'status': 'open', 'unrealized_pnl': None}, {'status': 'closed', 'unrealized_pnl': -8}],
        'trades': [{'action': 'SELL', 'pnl': -1}, {'action': 'BUY', 'pnl': 0}, {'action': 'SELL', 'pnl': 2}],
        'decisions': [], 'ranking': [], 'orders': [], 'price_history': {}, 'truncated': {}, 'limits': {},
    })
    with patch('app.services.momentum_ai_service.requests.post') as post:
        result = service.preview(30)
        assert result['metrics'] == {'open_positions': 2, 'losing_open_positions': 1,
                                     'missing_pnl_positions': 1, 'sell_trades': 2, 'losing_sell_trades': 1}
        service.snapshot.assert_called_once_with(30)
        post.assert_not_called()


def test_invalid_analysis_options_never_call_provider():
    with patch('app.services.momentum_ai_service.requests.post') as post:
        for options in ({'mode': 'unknown'}, {'days': 90}, {'question': 'x' * 1001}):
            with pytest.raises(HTTPException) as error:
                MomentumAIService(Mock()).analyze(**options)
            assert error.value.status_code == 422
        post.assert_not_called()


def test_snapshot_filters_transaction_history_by_selected_window():
    from datetime import timedelta
    from app.models.momentum_engine import MomentumEngineTrade
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        for name, days in [('recent', 2), ('old', 20)]:
            db.add(MomentumEngineTrade(trade_id=name, symbol='BTCUSD', action='SELL', price=10,
                                       created_at=datetime.now(timezone.utc) - timedelta(days=days)))
        db.commit()
        service = MomentumAIService(db)
        assert [t['trade_id'] for t in service.snapshot(7)['trades']] == ['recent']
        assert len(service.snapshot(30)['trades']) == 2


def test_tool_loop_returns_database_result_to_model_and_audits_call():
    import json
    service = MomentumAIService(Mock())
    service.snapshot = Mock(return_value={'positions': [1], 'ranking': [], 'trades': [],
        'as_of': datetime.now(timezone.utc).isoformat(), 'limits': {}, 'truncated': {}})
    tool_response = Mock()
    tool_response.json.return_value = {'status': 'completed', 'output': [
        {'type': 'function_call', 'name': 'read_momentum_data', 'call_id': 'call_1',
         'arguments': json.dumps({'dataset': 'trades', 'limit': 5})}]}
    report_response = Mock()
    report_response.json.return_value = {'status': 'completed', 'output': [
        {'type': 'message', 'content': [{'type': 'output_text', 'text': 'Rapport vérifié'}]}]}
    with patch('app.services.momentum_ai_service.settings') as config, \
         patch('app.services.momentum_ai_service.MomentumReadTools') as reader, \
         patch('app.services.momentum_ai_service.requests.post', side_effect=[tool_response, report_response]) as post:
        config.openai_api_key = 'key'
        config.openai_model = 'model'
        reader.return_value.read.return_value = {'items': [{'trade_id': 'evidence-1'}], 'has_more': False}
        result = service.analyze()
        assert result['report'] == 'Rapport vérifié'
        assert result['tool_calls'][0]['status'] == 'ok'
        reader.return_value.read.assert_called_once_with({'dataset': 'trades', 'limit': 5})
        outputs = post.call_args.kwargs['json']['input']
        assert any(item.get('type') == 'function_call_output' and 'evidence-1' in item['output'] for item in outputs)
        assert post.call_args.kwargs['json']['store'] is False


def test_read_tools_validate_and_scope_paginated_database_access():
    from app.services.momentum_ai_tools import MomentumReadTools
    from app.models.momentum_engine import MomentumEngineTrade
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        for name, scope in [('a', 'crypto'), ('b', 'crypto'), ('stock', 'stock_etf')]:
            db.add(MomentumEngineTrade(trade_id=name, market_scope=scope, symbol='BTCUSD', action='BUY', price=1))
        db.commit()
        reader = MomentumReadTools(db, datetime.now(timezone.utc).isoformat(), 7)
        page = reader.read({'dataset': 'trades', 'limit': 1})
        assert page['has_more'] and page['next_offset'] == 1
        second = reader.read({'dataset': 'trades', 'limit': 1, 'offset': 1})
        assert len(second['items']) == 1 and not second['has_more']
        assert page['items'][0]['trade_id'] != second['items'][0]['trade_id']
        assert {page['items'][0]['trade_id'], second['items'][0]['trade_id']} == {'a', 'b'}
        for invalid in ({'dataset': 'sql', 'query': 'DELETE FROM orders'}, {'dataset': 'trades', 'limit': 201}, {'dataset': 'candles'}):
            with pytest.raises(ValueError):
                reader.read(invalid)
        assert not db.new and not db.dirty and not db.deleted


def test_read_tools_reuse_existing_services():
    from app.services.momentum_ai_tools import MomentumReadTools
    with patch('app.services.momentum_ai_tools.MomentumService') as ranking, \
         patch('app.services.momentum_ai_tools.MomentumEngineService') as engine:
        ranking.return_value.list_rankings.return_value = [{'symbol': 'BTCUSD'}]
        engine.return_value.current_decision.return_value = {'decision_id': 'persisted'}
        reader = MomentumReadTools(Mock(), datetime.now(timezone.utc).isoformat(), 7)
        assert reader.read({'dataset': 'ranking'})['items'] == [{'symbol': 'BTCUSD'}]
        assert reader.read({'dataset': 'current_decision'})['data']['decision_id'] == 'persisted'
        ranking.return_value.list_rankings.assert_called_once()
        engine.return_value.current_decision.assert_called_once()


def test_candle_tool_respects_historical_cutoff_and_provider():
    from app.services.momentum_ai_tools import MomentumReadTools
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    now = datetime.now(timezone.utc)
    cutoff_ms = int(now.timestamp() * 1000) - 3600000
    with Session(engine) as db:
        for name, provider, stamp in [('old', 'KRAKEN', cutoff_ms-1), ('late', 'KRAKEN', cutoff_ms+1), ('other', 'IBKR', cutoff_ms-1)]:
            db.add(MarketCandle(candle_id=name, symbol='BTCUSD', provider=provider, interval='15m',
                               open_time=stamp-900000, close_time=stamp, open=1, high=1, low=1, close=1, volume=1))
        db.commit()
        reader = MomentumReadTools(db, now.isoformat(), 7)
        result = reader.read({'dataset': 'candles', 'symbol': 'BTCUSD',
                              'before': datetime.fromtimestamp(cutoff_ms/1000, timezone.utc).isoformat()})
        assert [row['candle_id'] for row in result['items']] == ['old']

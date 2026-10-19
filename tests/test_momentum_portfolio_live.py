from types import SimpleNamespace
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.models.base import Base
from app.services.execution import momentum_live_execution_service as module
from app.services.execution.momentum_live_execution_service import MomentumLiveExecutionService


class Exchange:
    def __init__(self, pending=False):
        self.cash = 900
        self.calls = []
        self.pending = pending
        self.client = SimpleNamespace(free_balance=lambda _: self.cash)
        self.rules = SimpleNamespace(quote_asset=lambda _: 'USDC')

    def buy_market(self, symbol, **kwargs):
        amount = kwargs['total_notional']
        self.calls.append(('buy', symbol, amount))
        self.cash -= amount
        return {'status': 'filled'}

    def sell_market(self, symbol, **kwargs):
        self.calls.append(('sell', symbol))
        return {'status': 'pending' if self.pending else 'filled'}


def configure(monkeypatch):
    monkeypatch.setattr(module, 'settings', SimpleNamespace(momentum_live_enabled=True,
        momentum_live_mode='spot', kraken_dry_run=False, kraken_quote_reserve=0,
        kraken_buy_balance_ratio=1, live_max_notional_per_trade=1000))
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    return Session(engine)


def batch(operations):
    return {'action': 'batch', 'decision_action': 'batch', 'decision_id': 'portfolio-test',
            'should_trade': True, 'due_now': True, 'operations': operations}


def test_live_splits_actual_cash_and_replay_does_not_repeat_operations(monkeypatch):
    with configure(monkeypatch) as db:
        exchange = Exchange()
        executor = MomentumLiveExecutionService(db, execution_service=exchange)
        plan = batch([{'action': 'buy', 'symbol': s, 'allocation_divisor': 3-i}
                      for i, s in enumerate(['BTCUSDC', 'ETHUSDC', 'SOLUSDC'])])
        assert executor.execute_decision(plan)['status'] == 'executed'
        executor.execute_decision(plan)
        assert exchange.calls == [('buy', 'BTCUSDC', 300), ('buy', 'ETHUSDC', 300), ('buy', 'SOLUSDC', 300)]


def test_live_pending_exit_blocks_buy_and_replay_does_not_resubmit_exit(monkeypatch):
    with configure(monkeypatch) as db:
        exchange = Exchange(pending=True)
        executor = MomentumLiveExecutionService(db, execution_service=exchange)
        plan = batch([{'action': 'sell', 'symbol': 'BTCUSDC'},
                      {'action': 'buy', 'symbol': 'ETHUSDC', 'allocation_divisor': 1}])
        assert executor.execute_decision(plan)['status'] == 'pending'
        assert executor.execute_decision(plan)['status'] == 'pending'
        assert exchange.calls == [('sell', 'BTCUSDC')]


def test_invalid_later_operation_is_rejected_before_any_live_order(monkeypatch):
    with configure(monkeypatch) as db:
        exchange = Exchange()
        executor = MomentumLiveExecutionService(db, execution_service=exchange)
        with pytest.raises(ValueError):
            executor.execute_decision(batch([{'action': 'sell', 'symbol': 'BTCUSDC'},
                {'action': 'buy', 'symbol': 'ETHUSDC', 'allocation_divisor': 0}]))
        assert exchange.calls == []


def test_partial_failure_retry_skips_successful_legs(monkeypatch):
    with configure(monkeypatch) as db:
        exchange = Exchange()
        executor = MomentumLiveExecutionService(db, execution_service=exchange)
        plan = batch([{'action': 'buy', 'symbol': s, 'allocation_divisor': 3-i}
                      for i, s in enumerate(['BTCUSDC', 'ETHUSDC', 'SOLUSDC'])])
        original = exchange.buy_market
        def interrupted(symbol, **kwargs):
            if symbol == 'ETHUSDC':
                raise RuntimeError('temporarily unavailable')
            return original(symbol, **kwargs)
        exchange.buy_market = interrupted
        with pytest.raises(RuntimeError):
            executor.execute_decision(plan)
        exchange.buy_market = original
        executor.execute_decision(plan)
        assert [c[1] for c in exchange.calls] == ['BTCUSDC', 'ETHUSDC', 'SOLUSDC']
        assert exchange.cash == pytest.approx(0)


def test_live_reserve_and_fee_buffer_applied_once_to_equal_batch_budget(monkeypatch):
    with configure(monkeypatch) as db:
        monkeypatch.setattr(module.settings, 'kraken_quote_reserve', 1)
        monkeypatch.setattr(module.settings, 'kraken_buy_balance_ratio', .995)
        exchange = Exchange()
        plan = batch([{'action': 'buy', 'symbol': s, 'allocation_divisor': 3-i}
                      for i, s in enumerate(['BTCUSDC', 'ETHUSDC', 'SOLUSDC'])])
        MomentumLiveExecutionService(db, execution_service=exchange).execute_decision(plan)
        assert [c[2] for c in exchange.calls] == pytest.approx([(900-1)*.995/3]*3)


def test_live_batch_accepts_configured_capacity_above_three(monkeypatch):
    with configure(monkeypatch) as db:
        monkeypatch.setattr(module.settings, 'momentum_max_positions', 4, raising=False)
        exchange = Exchange()
        plan = batch([{'action': 'buy', 'symbol': s, 'allocation_divisor': 4-i}
                      for i, s in enumerate(['BTCUSDC', 'ETHUSDC', 'SOLUSDC', 'XRPUSDC'])])
        MomentumLiveExecutionService(db, execution_service=exchange).execute_decision(plan)
        assert [c[2] for c in exchange.calls] == pytest.approx([225]*4)

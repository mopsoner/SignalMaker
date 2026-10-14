from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.momentum_service import MomentumService


def candles(prices):
    start = datetime.now(timezone.utc) - timedelta(days=4)
    return [dict(open_time=int((start + timedelta(minutes=15*i)).timestamp()*1000),
                 close_time=int((start + timedelta(minutes=15*(i+1))).timestamp()*1000),
                 open=p, high=p+1, low=p-1, close=p) for i, p in enumerate(prices)]


def test_long_history_removes_short_window_seed_bias_without_changing_other_inputs():
    service = object.__new__(MomentumService)
    history = candles([100]*168 + [130] + [100]*30 + [100.5])
    short = service._interval_momentum(history[-32:])
    stable = service._interval_momentum(history[-32:], ema_candles=history)
    assert short['ema_trend'] == 'below_ema'
    assert stable['ema_trend'] == 'above_ema'
    for key in ('price', 'change', 'rsi', 'candle_time', 'updated_at'):
        assert stable[key] == short[key]
    # Only the existing EMA bonus changes, not the price-change/RSI terms.
    assert stable['momentum'] - short['momentum'] == pytest.approx(10)


def test_ema_warmup_ignores_open_candle_and_uses_available_short_history():
    service = object.__new__(MomentumService)
    history = candles([100+i/10 for i in range(200)])
    baseline = service._interval_momentum(history[-32:], ema_candles=history)
    future = dict(history[-1], close=1e9, close_time=int((datetime.now(timezone.utc)+timedelta(minutes=15)).timestamp()*1000))
    assert service._interval_momentum(history[-32:], ema_candles=history+[future]) == baseline
    short = history[-20:]
    assert service._interval_momentum(short, ema_candles=short) == service._interval_momentum(short)


def test_crypto_loader_expands_only_ema_history_and_keeps_structure_window():
    service = object.__new__(MomentumService)
    history = candles([100+i/10 for i in range(201)])
    captured = {}
    def load(symbol, limits):
        captured.update(limits)
        return {'15m': history, '1h': history[-24:], '4h': history[-30:]}
    service.market_data = SimpleNamespace(load_symbol_bundle=load)
    row = service._build_symbol_row('TRXUSD')
    short = service._interval_momentum(history[-32:])
    assert captured == {'15m': 201, '1h': 24, '4h': 30}
    assert row['change_15m'] == short['change']
    assert row['rsi_15m'] == short['rsi']
    assert row['structure_15m'] == service._structure_15m(history[-32:])

from datetime import datetime, timezone
from statistics import mean

from app.services.runtime_settings import get_runtime_signal_config
from app.strategy.legacy_engine import build_signal


class SignalEngineService:
    def heartbeat(self) -> dict:
        return {
            'service': 'signal_engine',
            'status': 'ready',
            'last_tick_at': datetime.now(timezone.utc).isoformat(),
            'strategy': 'legacy_wyckoff_v231',
            'primary_interval': '5m',
        }

    def _range_position(self, price: float, low: float | None, high: float | None) -> float | None:
        if low is None or high is None or high <= low:
            return None
        return (price - low) / (high - low)

    def _near(self, price: float, level: float | None, pct: float) -> bool:
        if price <= 0 or level is None:
            return False
        return abs(price - level) / price <= pct

    def _macro_window_4h(self, signal: dict, cfg: dict) -> dict:
        price = float(signal.get('price') or 0.0)
        rsi_macro = signal.get('rsi_macro')
        range_low_4h = signal.get('range_low_4h')
        range_high_4h = signal.get('range_high_4h')
        previous_day_high = signal.get('previous_day_high')
        previous_day_low = signal.get('previous_day_low')
        previous_week_high = signal.get('previous_week_high')
        previous_week_low = signal.get('previous_week_low')
        resistance = ((signal.get('old_resistance_shelf') or {}).get('level'))
        support = ((signal.get('old_support_shelf') or {}).get('level'))

        pos = self._range_position(price, range_low_4h, range_high_4h)
        overbought = cfg['signals']['overbought']
        oversold = cfg['signals']['oversold']
        near_pct = max(cfg['signals'].get('price_near_extreme_pct', 0.004) * 4, 0.01)

        near_res_4h = any([
            self._near(price, resistance, near_pct),
            self._near(price, range_high_4h, near_pct),
            self._near(price, previous_day_high, near_pct),
            self._near(price, previous_week_high, near_pct),
        ])
        near_sup_4h = any([
            self._near(price, support, near_pct),
            self._near(price, range_low_4h, near_pct),
            self._near(price, previous_day_low, near_pct),
            self._near(price, previous_week_low, near_pct),
        ])

        bull_valid = False
        bear_valid = False
        bull_reasons = []
        bear_reasons = []
        if rsi_macro is not None and rsi_macro <= oversold:
            bull_valid = True
            bull_reasons.append('rsi_macro_oversold')
        if rsi_macro is not None and rsi_macro >= overbought:
            bear_valid = True
            bear_reasons.append('rsi_macro_overbought')
        if pos is not None and pos <= 0.40:
            bull_valid = True
            bull_reasons.append('price_in_discount_4h')
        if pos is not None and pos >= 0.60:
            bear_valid = True
            bear_reasons.append('price_in_premium_4h')
        if near_sup_4h:
            bull_valid = True
            bull_reasons.append('near_support_4h')
        if near_res_4h:
            bear_valid = True
            bear_reasons.append('near_resistance_4h')

        if bull_valid and not bear_valid:
            return {
                'valid': True,
                'side': 'bull',
                'reason': ','.join(bull_reasons),
                'range_position': pos,
                'range_high_4h': range_high_4h,
                'range_low_4h': range_low_4h,
                'near_resistance_4h': near_res_4h,
                'near_support_4h': near_sup_4h,
            }
        if bear_valid and not bull_valid:
            return {
                'valid': True,
                'side': 'bear',
                'reason': ','.join(bear_reasons),
                'range_position': pos,
                'range_high_4h': range_high_4h,
                'range_low_4h': range_low_4h,
                'near_resistance_4h': near_res_4h,
                'near_support_4h': near_sup_4h,
            }
        return {
            'valid': False,
            'side': 'neutral',
            'reason': 'no_clear_4h_trade_window',
            'range_position': pos,
            'range_high_4h': range_high_4h,
            'range_low_4h': range_low_4h,
            'near_resistance_4h': near_res_4h,
            'near_support_4h': near_sup_4h,
        }

    def _refinement_context_1h(self, signal: dict, candles_1h: list[dict]) -> dict:
        price = float(signal.get('price') or 0.0)
        bias = signal.get('bias') or 'neutral'
        entry_ctx = signal.get('entry_liquidity_context') or {}
        range_high_1h = signal.get('range_high_1h')
        range_low_1h = signal.get('range_low_1h')
        eq_highs = bool(signal.get('equal_highs_1h'))
        eq_lows = bool(signal.get('equal_lows_1h'))
        near_pct = 0.015

        recent_high = signal.get('macro_liquidity_context', {}).get('level') if 'high' in str((signal.get('macro_liquidity_context') or {}).get('type', '')) else None
        recent_low = signal.get('macro_liquidity_context', {}).get('level') if 'low' in str((signal.get('macro_liquidity_context') or {}).get('type', '')) else None
        if range_high_1h is not None:
            recent_high = range_high_1h if recent_high is None else max(recent_high, range_high_1h)
        if range_low_1h is not None:
            recent_low = range_low_1h if recent_low is None else min(recent_low, range_low_1h)

        h = [float(c['high']) for c in candles_1h[-6:]] if candles_1h else []
        l = [float(c['low']) for c in candles_1h[-6:]] if candles_1h else []
        c_last = candles_1h[-1] if candles_1h else None
        prev_high = max(h[:-1]) if len(h) > 1 else None
        prev_low = min(l[:-1]) if len(l) > 1 else None
        utad = bool(c_last and prev_high is not None and float(c_last['high']) > prev_high and float(c_last['close']) < prev_high)
        spring = bool(c_last and prev_low is not None and float(c_last['low']) < prev_low and float(c_last['close']) > prev_low)

        if bias.startswith('bear'):
            valid = any([
                self._near(price, entry_ctx.get('level'), near_pct),
                self._near(price, range_high_1h, near_pct),
                eq_highs,
                utad,
            ])
            return {
                'valid': valid,
                'side': 'bear',
                'reason': '1h_utad_or_resistance_retest' if valid else 'no_1h_bear_setup',
                'utad_watch_1h': utad,
                'spring_watch_1h': False,
                'near_range_high_1h': self._near(price, range_high_1h, near_pct),
                'near_range_low_1h': False,
                'equal_highs_1h': eq_highs,
                'equal_lows_1h': eq_lows,
            }
        if bias.startswith('bull'):
            valid = any([
                self._near(price, entry_ctx.get('level'), near_pct),
                self._near(price, range_low_1h, near_pct),
                eq_lows,
                spring,
            ])
            return {
                'valid': valid,
                'side': 'bull',
                'reason': '1h_spring_or_support_retest' if valid else 'no_1h_bull_setup',
                'utad_watch_1h': False,
                'spring_watch_1h': spring,
                'near_range_high_1h': False,
                'near_range_low_1h': self._near(price, range_low_1h, near_pct),
                'equal_highs_1h': eq_highs,
                'equal_lows_1h': eq_lows,
            }
        return {
            'valid': False,
            'side': 'neutral',
            'reason': 'neutral_bias',
            'utad_watch_1h': False,
            'spring_watch_1h': False,
            'near_range_high_1h': False,
            'near_range_low_1h': False,
            'equal_highs_1h': eq_highs,
            'equal_lows_1h': eq_lows,
        }

    def _execution_trigger_5m(self, signal: dict) -> dict:
        return {
            'valid': bool(signal.get('pipeline', {}).get('confirm')),
            'trigger': signal.get('trigger'),
            'confirm_source': signal.get('confirm_source'),
            'mss_bull': bool(signal.get('mss_bull')),
            'mss_bear': bool(signal.get('mss_bear')),
            'bos_bull': bool(signal.get('bos_bull')),
            'bos_bear': bool(signal.get('bos_bear')),
        }

    def _apply_hierarchy(self, signal: dict, candles_1h: list[dict], cfg: dict) -> dict:
        macro_window = self._macro_window_4h(signal, cfg)
        refinement = self._refinement_context_1h(signal, candles_1h)
        exec_trigger = self._execution_trigger_5m(signal)
        signal['macro_window_4h'] = macro_window
        signal['refinement_context_1h'] = refinement
        signal['execution_trigger_5m'] = exec_trigger
        signal['engine_name'] = 'legacy_wyckoff_v231_hierarchical'

        bias = signal.get('bias') or 'neutral'
        allowed = False
        block_reason = None
        if bias.startswith('bear'):
            if macro_window['valid'] and macro_window['side'] == 'bear':
                allowed = True
            else:
                block_reason = 'blocked_no_4h_bear_window'
        elif bias.startswith('bull'):
            if macro_window['valid'] and macro_window['side'] == 'bull':
                allowed = True
            else:
                block_reason = 'blocked_no_4h_bull_window'
        else:
            block_reason = 'blocked_neutral_bias'

        if allowed and not refinement['valid']:
            allowed = False
            block_reason = 'blocked_no_1h_setup'
        if allowed and not exec_trigger['valid']:
            allowed = False
            block_reason = 'blocked_no_5m_confirm'

        signal['hierarchy_block_reason'] = block_reason
        if not allowed:
            signal['trigger'] = 'wait'
            if bias.startswith('bear'):
                signal['bias'] = 'bear_watch'
                signal['state'] = 'resistance_retest_watch'
            elif bias.startswith('bull'):
                signal['bias'] = 'bull_watch'
                signal['state'] = 'support_retest_watch'
            else:
                signal['bias'] = 'neutral'
                signal['state'] = 'neutral_watch'
            signal['pipeline']['confirm'] = False
            signal['pipeline']['trade'] = False
            signal['trade'] = {
                'status': 'watch',
                'side': 'none',
                'entry': None,
                'stop': None,
                'target': None,
            }
        return signal

    def compute_signal(self, symbol: str, candles: dict[str, list[dict]]) -> dict:
        cfg = get_runtime_signal_config()
        candles_main = candles['5m']
        signal = build_signal(symbol, candles_main, candles_main, candles['1h'], candles['4h'], cfg)
        signal = self._apply_hierarchy(signal, candles['1h'], cfg)
        return signal

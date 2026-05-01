from datetime import datetime, timezone

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

    def _level(self, level_type: str, level: float | None, timeframe: str, side: str, source: str, quality: int) -> dict | None:
        if level is None:
            return None
        return {
            'type': level_type,
            'level': level,
            'timeframe': timeframe,
            'side': side,
            'source': source,
            'quality': quality,
        }

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
        near_pct = 0.012

        h = [float(c['high']) for c in candles_1h[-8:]] if candles_1h else []
        l = [float(c['low']) for c in candles_1h[-8:]] if candles_1h else []
        c_last = candles_1h[-1] if candles_1h else None
        last_high = float(c_last['high']) if c_last else None
        last_low = float(c_last['low']) if c_last else None
        last_close = float(c_last['close']) if c_last else None
        prev_high = max(h[:-1]) if len(h) > 1 else None
        prev_low = min(l[:-1]) if len(l) > 1 else None
        utad = bool(c_last and prev_high is not None and float(c_last['high']) > prev_high and float(c_last['close']) < prev_high)
        spring = bool(c_last and prev_low is not None and float(c_last['low']) < prev_low and float(c_last['close']) > prev_low)
        near_entry = self._near(price, entry_ctx.get('level'), near_pct)
        near_high = self._near(price, range_high_1h, near_pct)
        near_low = self._near(price, range_low_1h, near_pct)

        base = {
            'last_high_1h': last_high,
            'last_low_1h': last_low,
            'last_close_1h': last_close,
            'previous_high_1h': prev_high,
            'previous_low_1h': prev_low,
        }
        if bias.startswith('bear'):
            valid = bool(utad or near_high or (eq_highs and near_entry) or (near_entry and near_high))
            return {
                **base,
                'valid': valid,
                'side': 'bear',
                'reason': '1h_utad_or_resistance_retest' if valid else 'no_1h_bear_setup',
                'utad_watch_1h': utad,
                'spring_watch_1h': False,
                'near_range_high_1h': near_high,
                'near_range_low_1h': False,
                'equal_highs_1h': eq_highs,
                'equal_lows_1h': eq_lows,
            }
        if bias.startswith('bull'):
            valid = bool(spring or near_low or (eq_lows and near_entry) or (near_entry and near_low))
            return {
                **base,
                'valid': valid,
                'side': 'bull',
                'reason': '1h_spring_or_support_retest' if valid else 'no_1h_bull_setup',
                'utad_watch_1h': False,
                'spring_watch_1h': spring,
                'near_range_high_1h': False,
                'near_range_low_1h': near_low,
                'equal_highs_1h': eq_highs,
                'equal_lows_1h': eq_lows,
            }
        return {
            **base,
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

    def _wyckoff_event_level(self, signal: dict) -> dict:
        bias = signal.get('bias') or 'neutral'
        price = float(signal.get('price') or 0.0)
        macro_ctx = signal.get('macro_liquidity_context') or signal.get('liquidity_context') or {}
        entry_ctx = signal.get('entry_liquidity_context') or {}
        old_res = signal.get('old_resistance_shelf') or {}
        old_sup = signal.get('old_support_shelf') or {}
        candidates = []

        if bias.startswith('bear'):
            raw = [
                self._level(macro_ctx.get('type', 'macro_context'), macro_ctx.get('level'), macro_ctx.get('timeframe') or '4h', 'bear', 'macro_context', 100),
                self._level('previous_week_high', signal.get('previous_week_high'), '1w', 'bear', 'previous_week', 90),
                self._level('previous_day_high', signal.get('previous_day_high'), '1d', 'bear', 'previous_day', 85),
                self._level('range_high_4h', signal.get('range_high_4h'), '4h', 'bear', 'range', 80),
                self._level('major_swing_high_4h', signal.get('major_swing_high_4h'), '4h', 'bear', 'swing', 75),
                self._level(old_res.get('type', 'old_resistance_shelf'), old_res.get('level'), old_res.get('timeframe') or '4h', 'bear', 'shelf', 70),
                self._level('range_high_1h', signal.get('range_high_1h'), '1h', 'bear', 'range', 60),
                self._level(entry_ctx.get('type', 'entry_context'), entry_ctx.get('level'), entry_ctx.get('timeframe') or '5m', 'bear', 'entry_fallback', 20),
            ]
            candidates = [c for c in raw if c and c.get('level') is not None]
        elif bias.startswith('bull'):
            raw = [
                self._level(macro_ctx.get('type', 'macro_context'), macro_ctx.get('level'), macro_ctx.get('timeframe') or '4h', 'bull', 'macro_context', 100),
                self._level('previous_week_low', signal.get('previous_week_low'), '1w', 'bull', 'previous_week', 90),
                self._level('previous_day_low', signal.get('previous_day_low'), '1d', 'bull', 'previous_day', 85),
                self._level('range_low_4h', signal.get('range_low_4h'), '4h', 'bull', 'range', 80),
                self._level('major_swing_low_4h', signal.get('major_swing_low_4h'), '4h', 'bull', 'swing', 75),
                self._level(old_sup.get('type', 'old_support_shelf'), old_sup.get('level'), old_sup.get('timeframe') or '4h', 'bull', 'shelf', 70),
                self._level('range_low_1h', signal.get('range_low_1h'), '1h', 'bull', 'range', 60),
                self._level(entry_ctx.get('type', 'entry_context'), entry_ctx.get('level'), entry_ctx.get('timeframe') or '5m', 'bull', 'entry_fallback', 20),
            ]
            candidates = [c for c in raw if c and c.get('level') is not None]

        if not candidates:
            return {'valid': False, 'type': 'none', 'level': None, 'reason': 'no_wyckoff_event_level'}

        seen = set()
        deduped = []
        for c in candidates:
            key = (c['type'], c['timeframe'], round(float(c['level']), 12))
            if key not in seen:
                seen.add(key)
                distance_pct = abs(price - float(c['level'])) / price if price > 0 else None
                c['distance_pct'] = distance_pct
                deduped.append(c)

        # Prefer structural HTF levels, then closest distance. 5m is only a fallback.
        selected = sorted(deduped, key=lambda x: (-int(x.get('quality', 0)), x.get('distance_pct') if x.get('distance_pct') is not None else 999))[0]
        level = float(selected['level'])
        if bias.startswith('bear'):
            selected['swept'] = bool(price > level)
            selected['reclaimed'] = bool(price < level)
        elif bias.startswith('bull'):
            selected['swept'] = bool(price < level)
            selected['reclaimed'] = bool(price > level)
        selected['valid'] = True
        selected['reason'] = 'structural Wyckoff event level selected before local entry context'
        return selected

    def _wyckoff_requirement(self, signal: dict) -> dict:
        bias = signal.get('bias') or 'neutral'
        macro_window = signal.get('macro_window_4h') or {}
        refinement = signal.get('refinement_context_1h') or {}
        exec_trigger = signal.get('execution_trigger_5m') or {}
        event_level = signal.get('wyckoff_event_level') or {}
        price = float(signal.get('price') or 0.0)
        level = event_level.get('level')
        last_high = refinement.get('last_high_1h')
        last_low = refinement.get('last_low_1h')
        last_close = refinement.get('last_close_1h')
        distance_pct = abs(price - level) / price if price > 0 and level is not None else None

        if bias.startswith('bear'):
            expected = 'utad'
            swept = bool(level is not None and ((last_high is not None and last_high > level) or price > level))
            rejected = bool(swept and level is not None and ((last_close is not None and last_close < level) or price < level))
            event_confirmed = bool(refinement.get('utad_watch_1h') or rejected)
            setup_ready = bool(swept or event_confirmed)
            if event_confirmed and exec_trigger.get('valid'):
                status = 'execution_ready'
                reason = 'UTAD/rejection confirmed and 5m structure confirmed'
            elif event_confirmed:
                status = 'rejected_waiting_5m_confirm'
                reason = 'liquidity swept, rejection detected, waiting for 5m MSS/BOS bear'
            elif swept:
                status = 'swept_waiting_rejection'
                reason = 'price swept structural resistance, waiting for rejection below level or 5m breakdown'
            else:
                status = 'waiting_sweep'
                reason = 'bear window valid, waiting for sweep/UTAD of structural resistance'
        elif bias.startswith('bull'):
            expected = 'spring'
            swept = bool(level is not None and ((last_low is not None and last_low < level) or price < level))
            reclaimed = bool(swept and level is not None and ((last_close is not None and last_close > level) or price > level))
            event_confirmed = bool(refinement.get('spring_watch_1h') or reclaimed)
            setup_ready = bool(swept or event_confirmed)
            if event_confirmed and exec_trigger.get('valid'):
                status = 'execution_ready'
                reason = 'Spring/reclaim confirmed and 5m structure confirmed'
            elif event_confirmed:
                status = 'reclaimed_waiting_5m_confirm'
                reason = 'liquidity swept, reclaim detected, waiting for 5m MSS/BOS bull'
            elif swept:
                status = 'swept_waiting_reclaim'
                reason = 'price swept structural support, waiting for reclaim above level or 5m reclaim'
            else:
                status = 'waiting_sweep'
                reason = 'bull window valid, waiting for sweep/Spring of structural support'
        else:
            expected = 'none'
            swept = False
            event_confirmed = False
            setup_ready = False
            status = 'not_required'
            reason = 'neutral bias'

        if not macro_window.get('valid'):
            status = 'blocked'
            reason = '4h window not valid for Wyckoff event'

        return {
            'needed': bias.startswith('bull') or bias.startswith('bear'),
            'expected': expected,
            'status': status,
            'confirmed': event_confirmed,
            'setup_ready': setup_ready,
            'swept': swept,
            'timeframe': event_level.get('timeframe') or '1h',
            'event_level': event_level,
            'entry_level': level,
            'distance_pct': distance_pct,
            'reason': reason,
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

    def _preferred_macro_context(self, signal: dict) -> dict:
        bias = signal.get('bias') or 'neutral'
        price = float(signal.get('price') or 0.0)
        prev_day_high = signal.get('previous_day_high')
        prev_day_low = signal.get('previous_day_low')
        prev_week_high = signal.get('previous_week_high')
        prev_week_low = signal.get('previous_week_low')
        range_high_4h = signal.get('range_high_4h')
        range_low_4h = signal.get('range_low_4h')
        swing_high = signal.get('major_swing_high_4h')
        swing_low = signal.get('major_swing_low_4h')
        old_res = signal.get('old_resistance_shelf')
        old_sup = signal.get('old_support_shelf')

        if bias.startswith('bear'):
            candidates = [
                ({'type': 'old_resistance_shelf', 'level': old_res.get('level'), 'reason': '4h resistance shelf used as macro sell-side context', 'timeframe': '4h', 'scope': 'macro'} if old_res and old_res.get('level') is not None and old_res.get('level') >= price * 0.95 else None),
                ({'type': 'range_high_4h', 'level': range_high_4h, 'reason': '4h range high used as macro sell-side context', 'timeframe': '4h', 'scope': 'macro'} if range_high_4h is not None else None),
                ({'type': 'major_swing_high_4h', 'level': swing_high, 'reason': 'major 4h swing high used as macro sell-side context', 'timeframe': '4h', 'scope': 'macro'} if swing_high is not None else None),
                ({'type': 'previous_day_high', 'level': prev_day_high, 'reason': 'previous day high used as macro sell-side context', 'timeframe': '1d', 'scope': 'macro'} if prev_day_high is not None else None),
                ({'type': 'previous_week_high', 'level': prev_week_high, 'reason': 'previous week high used as macro sell-side context', 'timeframe': '1w', 'scope': 'macro'} if prev_week_high is not None else None),
            ]
        elif bias.startswith('bull'):
            candidates = [
                ({'type': 'old_support_shelf', 'level': old_sup.get('level'), 'reason': '4h support shelf used as macro buy-side context', 'timeframe': '4h', 'scope': 'macro'} if old_sup and old_sup.get('level') is not None and old_sup.get('level') <= price * 1.05 else None),
                ({'type': 'range_low_4h', 'level': range_low_4h, 'reason': '4h range low used as macro buy-side context', 'timeframe': '4h', 'scope': 'macro'} if range_low_4h is not None else None),
                ({'type': 'major_swing_low_4h', 'level': swing_low, 'reason': 'major 4h swing low used as macro buy-side context', 'timeframe': '4h', 'scope': 'macro'} if swing_low is not None else None),
                ({'type': 'previous_day_low', 'level': prev_day_low, 'reason': 'previous day low used as macro buy-side context', 'timeframe': '1d', 'scope': 'macro'} if prev_day_low is not None else None),
                ({'type': 'previous_week_low', 'level': prev_week_low, 'reason': 'previous week low used as macro buy-side context', 'timeframe': '1w', 'scope': 'macro'} if prev_week_low is not None else None),
            ]
        else:
            candidates = []
        for item in candidates:
            if item is not None and item.get('level') is not None:
                return item
        return signal.get('macro_liquidity_context') or signal.get('liquidity_context') or {'type': 'none', 'level': None, 'reason': 'no macro context', 'timeframe': None, 'scope': 'macro'}

    def _coherent_state(self, signal: dict) -> tuple[str, str]:
        bias = signal.get('bias') or 'neutral'
        rsi_htf = signal.get('rsi_htf')
        macro_window = signal.get('macro_window_4h') or {}
        wyckoff = signal.get('wyckoff_requirement') or {}
        status = wyckoff.get('status')
        if bias.startswith('bull'):
            if rsi_htf is not None and rsi_htf >= 78:
                return 'liquidity', 'neutral_watch'
            if macro_window.get('valid'):
                if status == 'execution_ready':
                    return 'spring_execution_ready', 'spring_execution_ready'
                if status == 'reclaimed_waiting_5m_confirm':
                    return 'spring_confirmed_watch', 'spring_confirmed_watch'
                if status == 'swept_waiting_reclaim':
                    return 'awaiting_reclaim_watch', 'awaiting_reclaim_watch'
                if status == 'waiting_sweep':
                    return 'awaiting_spring_watch', 'awaiting_spring_watch'
                if 'discount' in str(macro_window.get('reason', '')) or macro_window.get('near_support_4h'):
                    return 'discount_4h_reclaim_watch', 'support_retest_watch'
        if bias.startswith('bear'):
            if rsi_htf is not None and rsi_htf <= 22:
                return 'liquidity', 'neutral_watch'
            if macro_window.get('valid'):
                if status == 'execution_ready':
                    return 'utad_execution_ready', 'utad_execution_ready'
                if status == 'rejected_waiting_5m_confirm':
                    return 'utad_confirmed_watch', 'utad_confirmed_watch'
                if status == 'swept_waiting_rejection':
                    return 'awaiting_rejection_watch', 'awaiting_rejection_watch'
                if status == 'waiting_sweep':
                    return 'awaiting_utad_watch', 'awaiting_utad_watch'
                if 'premium' in str(macro_window.get('reason', '')) or macro_window.get('near_resistance_4h'):
                    return 'premium_4h_rejection_watch', 'resistance_retest_watch'
        return signal.get('state') or 'neutral_watch', signal.get('state') or 'neutral_watch'

    def _zone_validity(self, signal: dict) -> dict:
        macro_window = signal.get('macro_window_4h') or {}
        wyckoff = signal.get('wyckoff_requirement') or {}
        exec_target = (signal.get('execution_target') or {}).get('level')
        projected_target = (signal.get('projected_target') or {}).get('level')
        target_ok = exec_target is not None or projected_target is not None
        wyckoff_ok = wyckoff.get('status') in {
            'swept_waiting_rejection',
            'swept_waiting_reclaim',
            'rejected_waiting_5m_confirm',
            'reclaimed_waiting_5m_confirm',
            'execution_ready',
        }
        score = 0
        if macro_window.get('valid'):
            score += 2
        if wyckoff_ok:
            score += 2
        if target_ok:
            score += 2
        valid = score >= 5
        return {
            'valid': valid,
            'score': score,
            'target_ok': target_ok,
            'wyckoff_ok': wyckoff_ok,
            'reason': 'valid_zone' if valid else 'weak_zone_filters',
        }

    def _compute_final_score(self, signal: dict) -> tuple[float, dict]:
        base_score = float(signal.get('legacy_score', signal.get('score') or 0.0))
        macro_window = signal.get('macro_window_4h') or {}
        refinement = signal.get('refinement_context_1h') or {}
        exec_trigger = signal.get('execution_trigger_5m') or {}
        zone_validity = signal.get('zone_validity') or {}
        wyckoff = signal.get('wyckoff_requirement') or {}
        pipeline = signal.get('pipeline') or {}
        state = signal.get('state') or ''
        bias = signal.get('bias') or ''
        block_reason = signal.get('hierarchy_block_reason')

        adjustments = {
            'macro_window': 0.0,
            'refinement': 0.0,
            'wyckoff': 0.0,
            'zone_validity': 0.0,
            'confirm': 0.0,
            'trade': 0.0,
            'state_fit': 0.0,
            'block_penalty': 0.0,
        }

        if macro_window.get('valid'):
            adjustments['macro_window'] += 2.0
            if macro_window.get('side') != 'neutral' and bias.startswith(macro_window.get('side', '')):
                adjustments['macro_window'] += 1.0
        else:
            adjustments['block_penalty'] -= 2.0

        if refinement.get('valid'):
            adjustments['refinement'] += 1.0
        else:
            adjustments['refinement'] -= 0.5

        wyckoff_status = wyckoff.get('status')
        if wyckoff_status == 'execution_ready':
            adjustments['wyckoff'] += 3.0
        elif wyckoff_status in {'rejected_waiting_5m_confirm', 'reclaimed_waiting_5m_confirm'}:
            adjustments['wyckoff'] += 2.5
        elif wyckoff_status in {'swept_waiting_rejection', 'swept_waiting_reclaim'}:
            adjustments['wyckoff'] += 1.5
        elif wyckoff_status == 'waiting_sweep':
            adjustments['wyckoff'] -= 0.5
        elif wyckoff_status == 'blocked':
            adjustments['wyckoff'] -= 1.5

        if zone_validity.get('valid'):
            adjustments['zone_validity'] += 1.5
        else:
            adjustments['zone_validity'] -= 1.0

        if exec_trigger.get('valid'):
            adjustments['confirm'] += 2.0
        if pipeline.get('trade'):
            adjustments['trade'] += 3.0
        elif pipeline.get('confirm'):
            adjustments['trade'] += 1.0

        if state in {'spring_execution_ready', 'utad_execution_ready'}:
            adjustments['state_fit'] += 2.0
        elif state in {'spring_confirmed_watch', 'utad_confirmed_watch'}:
            adjustments['state_fit'] += 1.5
        elif state in {'awaiting_reclaim_watch', 'awaiting_rejection_watch'}:
            adjustments['state_fit'] += 1.0
        elif state in {'awaiting_spring_watch', 'awaiting_utad_watch'}:
            adjustments['state_fit'] += 0.5
        elif state in {'discount_4h_reclaim_watch', 'premium_4h_rejection_watch'}:
            adjustments['state_fit'] += 0.5
        elif state == 'neutral_watch':
            adjustments['state_fit'] -= 1.0

        if block_reason == 'blocked_no_5m_confirm':
            adjustments['block_penalty'] -= 0.25
        elif block_reason == 'blocked_no_wyckoff_event':
            adjustments['block_penalty'] -= 0.75
        elif block_reason in {'blocked_no_1h_setup', 'blocked_no_4h_bull_window', 'blocked_no_4h_bear_window'}:
            adjustments['block_penalty'] -= 1.25

        final_score = max(0.0, base_score + sum(adjustments.values()))
        return final_score, adjustments

    def _apply_hierarchy(self, signal: dict, candles_1h: list[dict], candles_5m: list[dict], cfg: dict) -> dict:
        macro_window = self._macro_window_4h(signal, cfg)
        refinement = self._refinement_context_1h(signal, candles_1h)
        exec_trigger = self._execution_trigger_5m(signal)
        signal['macro_window_4h'] = macro_window
        signal['refinement_context_1h'] = refinement
        signal['execution_trigger_5m'] = exec_trigger
        signal['engine_name'] = 'legacy_wyckoff_v231_hierarchical'
        signal['macro_liquidity_context'] = self._preferred_macro_context(signal)
        signal['liquidity_context'] = signal['macro_liquidity_context']
        signal['wyckoff_event_level'] = self._wyckoff_event_level(signal)
        signal['wyckoff_requirement'] = self._wyckoff_requirement(signal)

        bias = signal.get('bias') or 'neutral'
        wyckoff = signal.get('wyckoff_requirement') or {}
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

        if allowed and wyckoff.get('status') not in {'rejected_waiting_5m_confirm', 'reclaimed_waiting_5m_confirm', 'execution_ready'}:
            allowed = False
            block_reason = 'blocked_no_wyckoff_event'
        if allowed and not exec_trigger['valid']:
            allowed = False
            block_reason = 'blocked_no_5m_confirm'

        state_label, fallback_state = self._coherent_state(signal)
        zone_validity = self._zone_validity(signal)
        signal['zone_validity'] = zone_validity
        signal['hierarchy_block_reason'] = block_reason

        if signal.get('pipeline', {}).get('zone'):
            if not zone_validity.get('valid'):
                signal['pipeline']['zone'] = False
                signal['zone_quality'] = 'weak'
                signal['state'] = fallback_state if fallback_state != 'neutral_watch' else 'neutral_watch'
            else:
                signal['state'] = state_label

        if not allowed:
            signal['trigger'] = 'wait'
            if bias.startswith('bear'):
                signal['bias'] = 'bear_watch'
                signal['state'] = state_label if state_label != 'neutral_watch' else 'awaiting_utad_watch'
            elif bias.startswith('bull'):
                signal['bias'] = 'bull_watch'
                signal['state'] = state_label if state_label != 'neutral_watch' else 'awaiting_spring_watch'
            else:
                signal['bias'] = 'neutral'
                signal['state'] = 'neutral_watch'
            signal['pipeline']['confirm'] = False
            signal['pipeline']['trade'] = False
            signal['trade'] = {'status': 'watch', 'side': 'none', 'entry': None, 'stop': None, 'target': None}

        signal['legacy_score'] = float(signal.get('score') or 0.0)
        signal['legacy_score_breakdown'] = dict(signal.get('score_breakdown') or {})
        final_score, final_score_breakdown = self._compute_final_score(signal)
        signal['final_score'] = final_score
        signal['final_score_breakdown'] = final_score_breakdown
        signal['score'] = final_score
        return signal

    def compute_signal(self, symbol: str, candles: dict[str, list[dict]]) -> dict:
        cfg = get_runtime_signal_config()
        candles_main = candles['5m']
        signal = build_signal(symbol, candles_main, candles_main, candles['1h'], candles['4h'], cfg)
        signal = self._apply_hierarchy(signal, candles['1h'], candles_main, cfg)
        return signal

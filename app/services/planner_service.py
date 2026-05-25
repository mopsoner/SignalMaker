from datetime import datetime, timezone

from app.services.runtime_settings import load_runtime_settings

STOP_BUFFER_PCT = 0.002


class PlannerService:
    def heartbeat(self) -> dict:
        runtime = load_runtime_settings()
        strategy = runtime['strategy']
        return {
            'service': 'planner',
            'status': 'ready',
            'last_tick_at': datetime.now(timezone.utc).isoformat(),
            'min_score': strategy['planner_min_score'],
            'min_rr': strategy['planner_min_rr'],
            'stop_policy': 'structural_invalidation_with_buffer',
            'stop_buffer_pct': STOP_BUFFER_PCT,
            'target_policy': 'structural_liquidity',
            'execution_policy': '1h_setup_15m_alignment_required',
            'side_policy': 'candidate_side_backward_compatible__payload_trade_normalized',
        }

    def _as_float(self, value):
        if isinstance(value, dict):
            value = value.get('level')
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if number > 0 else None

    def _distance_pct(self, entry: float, level: float | None) -> float | None:
        if entry <= 0 or level is None:
            return None
        return abs(float(level) - entry) / entry

    def _side_from_signal(self, signal: dict, trade: dict) -> str | None:
        raw_side = (trade.get('position_side') or trade.get('side') or trade.get('entry_action') or trade.get('order_side') or '').lower()
        if raw_side in {'long', 'buy', 'bull'}:
            return 'long'
        if raw_side in {'short', 'sell', 'bear'}:
            return 'short'
        bias = (signal.get('bias') or '').lower()
        if bias.startswith('bull'):
            return 'long'
        if bias.startswith('bear'):
            return 'short'
        gate_side = ((signal.get('hierarchy_gate') or {}).get('side') or '').lower()
        if gate_side == 'bull':
            return 'long'
        if gate_side == 'bear':
            return 'short'
        return None

    def _side_fields(self, position_side: str) -> dict:
        if position_side == 'short':
            return {
                'side': 'short',
                'position_side': 'short',
                'entry_action': 'sell',
                'exit_action': 'buy',
                'order_side': 'sell',
                'side_label': 'SHORT',
            }
        return {
            'side': 'long',
            'position_side': 'long',
            'entry_action': 'buy',
            'exit_action': 'sell',
            'order_side': 'buy',
            'side_label': 'LONG',
        }

    def _watch_reason(self, signal: dict, trade: dict) -> str:
        if signal.get('hierarchy_block_reason'):
            return signal['hierarchy_block_reason']
        state = signal.get('state') or 'watch'
        exec_trigger = signal.get('execution_trigger') or signal.get('execution_trigger_5m') or {}
        if exec_trigger.get('alignment_status') == 'opposed':
            return f"watch_15m_opposes_1h_setup:{exec_trigger.get('alignment_reason', state)}"
        if not signal.get('pipeline', {}).get('confirm'):
            return f'watch_not_confirmed:{state}'
        if (signal.get('execution_target') or {}).get('level') is None:
            return 'watch_missing_target_projection'
        if (signal.get('entry_liquidity_context') or {}).get('level') is None:
            return 'watch_missing_entry_context'
        return f"watch_missing_entry:{signal.get('bias') or state}"

    def _can_infer_candidate(self, signal: dict) -> bool:
        exec_trigger = signal.get('execution_trigger') or signal.get('execution_trigger_5m') or {}
        if exec_trigger.get('alignment_status') == 'opposed' or signal.get('confirm_blocked_by_hierarchy'):
            return False
        gate = signal.get('hierarchy_gate') or {}
        return bool(gate.get('accepted') is True or signal.get('pipeline', {}).get('confirm'))

    def _add_stop_candidate(self, candidates: list[dict], *, name: str, level, entry: float, side: str, rank: int) -> None:
        source_level = self._as_float(level)
        if source_level is None:
            return
        if side == 'short':
            stop_level = source_level * (1.0 + STOP_BUFFER_PCT)
            if stop_level <= entry:
                return
            method = 'above_source_plus_buffer'
        elif side == 'long':
            stop_level = source_level * (1.0 - STOP_BUFFER_PCT)
            if stop_level >= entry:
                return
            method = 'below_source_minus_buffer'
        else:
            return
        candidates.append({
            'source': name,
            'source_level': source_level,
            'level': stop_level,
            'distance': abs(stop_level - entry),
            'distance_pct': self._distance_pct(entry, stop_level),
            'buffer_pct': STOP_BUFFER_PCT,
            'method': method,
            'hierarchy_rank': rank,
            'valid': True,
            'validation': 'structural_stop',
            'rejected_reason': None,
        })

    def _infer_stop(self, signal: dict, *, side: str, entry: float) -> tuple[float | None, str | None, list[dict]]:
        candidates: list[dict] = []
        entry_ctx = signal.get('entry_liquidity_context') or {}
        macro_ctx = signal.get('macro_liquidity_context') or signal.get('liquidity_context') or {}
        if side == 'short':
            sources = [
                ('entry_liquidity_context', entry_ctx, 5),
                ('external_swing_high', signal.get('external_swing_high'), 20),
                ('internal_bear_pivot_high', signal.get('internal_bear_pivot_high'), 30),
                ('range_high_1h', signal.get('range_high_1h'), 40),
                ('previous_day_high', signal.get('previous_day_high'), 50),
                ('macro_liquidity_context', macro_ctx, 60),
                ('range_high_4h', signal.get('range_high_4h'), 70),
                ('major_swing_high_4h', signal.get('major_swing_high_4h'), 80),
            ]
        else:
            sources = [
                ('entry_liquidity_context', entry_ctx, 5),
                ('external_swing_low', signal.get('external_swing_low'), 20),
                ('internal_bull_pivot_low', signal.get('internal_bull_pivot_low'), 30),
                ('range_low_1h', signal.get('range_low_1h'), 40),
                ('previous_day_low', signal.get('previous_day_low'), 50),
                ('macro_liquidity_context', macro_ctx, 60),
                ('range_low_4h', signal.get('range_low_4h'), 70),
                ('major_swing_low_4h', signal.get('major_swing_low_4h'), 80),
            ]
        for name, value, rank in sources:
            self._add_stop_candidate(candidates, name=name, level=value, entry=entry, side=side, rank=rank)
        ordered = sorted(candidates, key=lambda item: (item['hierarchy_rank'], item.get('distance_pct') or 999))
        if ordered:
            selected = ordered[0]
            return selected['level'], selected['source'], ordered
        return None, 'missing_structural_stop', ordered

    def _add_target_candidate(self, candidates: list[dict], *, name: str, value, entry: float, side: str, rank: int) -> None:
        level = self._as_float(value)
        if level is None:
            return
        if side == 'short' and level >= entry:
            return
        if side == 'long' and level <= entry:
            return
        candidates.append({
            'source': name,
            'level': level,
            'distance': abs(level - entry),
            'distance_pct': self._distance_pct(entry, level),
            'hierarchy_rank': rank,
            'valid': True,
            'validation': 'structural_liquidity_target',
            'rejected_reason': None,
        })

    def _infer_target(self, signal: dict, *, side: str, entry: float) -> tuple[float | None, str | None, list[dict]]:
        candidates: list[dict] = []
        ordered_sources = [
            ('execution_target', signal.get('execution_target')),
            ('projected_target', signal.get('projected_target')),
            ('context_selection_debug.selected_target', (signal.get('context_selection_debug') or {}).get('selected_target')),
        ]
        for index, item in enumerate((signal.get('context_selection_debug') or {}).get('target_candidates') or []):
            ordered_sources.append((f'context_selection_debug.target_candidates[{index}]', item))
        fallback = ['range_low_1h', 'previous_day_low', 'old_support_shelf', 'previous_week_low', 'range_low_4h', 'major_swing_low_4h'] if side == 'short' else ['range_high_1h', 'previous_day_high', 'old_resistance_shelf', 'previous_week_high', 'range_high_4h', 'major_swing_high_4h']
        ordered_sources.extend((key, signal.get(key)) for key in fallback)
        seen = set()
        rank = 10
        for source, value in ordered_sources:
            level = self._as_float(value)
            if level is not None:
                key = round(level, 12)
                if key in seen:
                    continue
                seen.add(key)
            self._add_target_candidate(candidates, name=source, value=value, entry=entry, side=side, rank=rank)
            rank += 10
        ordered = sorted(candidates, key=lambda item: (item['hierarchy_rank'], item.get('distance_pct') or 999))
        if ordered:
            selected = ordered[0]
            return selected['level'], selected['source'], ordered
        return None, 'missing_structural_target', ordered

    def _resolve_trade_plan(self, signal: dict, trade: dict) -> tuple[dict, str | None]:
        side = self._side_from_signal(signal, trade)
        if not side:
            return {}, 'missing_side'
        entry = self._as_float(trade.get('entry'))
        entry_source = 'trade.entry'
        if entry is None and self._can_infer_candidate(signal):
            entry = self._as_float(signal.get('price'))
            entry_source = 'signal.price'
        if entry is None:
            return {**self._side_fields(side)}, self._watch_reason(signal, trade)
        stop = self._as_float(trade.get('stop'))
        stop_source = trade.get('stop_source') or 'trade.stop'
        stop_candidates = []
        if stop is None:
            stop, stop_source, stop_candidates = self._infer_stop(signal, side=side, entry=entry)
        if stop is None:
            return {'entry': entry, 'stop_candidates': stop_candidates, **self._side_fields(side)}, stop_source or 'missing_structural_stop'
        target = self._as_float(trade.get('target'))
        target_source = trade.get('target_source') or 'trade.target'
        target_candidates = []
        if target is None:
            target, target_source, target_candidates = self._infer_target(signal, side=side, entry=entry)
        if target is None:
            return {'entry': entry, 'stop': stop, 'stop_candidates': stop_candidates, 'target_candidates': target_candidates, **self._side_fields(side)}, target_source or 'missing_structural_target'
        if side == 'short':
            if stop <= entry:
                return {'entry': entry, 'stop': stop, 'target': target, **self._side_fields(side)}, 'invalid_short_stop'
            if target >= entry:
                return {'entry': entry, 'stop': stop, 'target': target, **self._side_fields(side)}, 'invalid_short_target'
        else:
            if stop >= entry:
                return {'entry': entry, 'stop': stop, 'target': target, **self._side_fields(side)}, 'invalid_long_stop'
            if target <= entry:
                return {'entry': entry, 'stop': stop, 'target': target, **self._side_fields(side)}, 'invalid_long_target'
        resolved = {
            **self._side_fields(side),
            'status': 'planned',
            'entry': entry,
            'stop': stop,
            'target': target,
            'entry_source': entry_source,
            'stop_source': stop_source,
            'target_source': target_source,
            'stop_distance_pct': self._distance_pct(entry, stop),
            'target_distance_pct': self._distance_pct(entry, target),
            'stop_validation': 'structural_invalidation',
            'target_validation': 'structural_liquidity',
            'inferred_by': 'planner_from_accepted_signal_v6_upsert_compatible_candidate',
        }
        if stop_candidates:
            resolved['stop_candidates'] = stop_candidates[:8]
        if target_candidates:
            resolved['target_candidates'] = target_candidates[:8]
        return resolved, None

    def assess_signal(self, signal: dict) -> dict:
        runtime = load_runtime_settings()
        strategy = runtime['strategy']
        trade = signal.get('trade', {}) or {}
        score = float(signal.get('score', 0.0))
        resolved_trade, error_reason = self._resolve_trade_plan(signal, trade)
        if error_reason:
            signal['trade_plan_rejected'] = resolved_trade
            return {'accepted': False, 'reason': error_reason, 'candidate': None}
        side = resolved_trade['position_side']
        entry = resolved_trade['entry']
        stop = resolved_trade['stop']
        target = resolved_trade['target']
        risk = abs(entry - stop)
        reward = abs(target - entry)
        rr = reward / risk if risk > 0 else None
        if risk <= 0:
            return {'accepted': False, 'reason': 'invalid_risk', 'candidate': None, 'rr_ratio': rr}
        if score < strategy['planner_min_score']:
            return {'accepted': False, 'reason': 'low_score', 'candidate': None, 'rr_ratio': rr}
        if rr is None or rr < strategy['planner_min_rr']:
            return {'accepted': False, 'reason': 'low_rr', 'candidate': None, 'rr_ratio': rr}
        signal['trade'] = resolved_trade
        signal['planner_trade_plan'] = {'model': 'accepted_signal_inference_v6_upsert_compatible_candidate', **resolved_trade, 'rr_ratio': rr}
        signal.setdefault('pipeline', {})['trade'] = True
        stage = 'trade' if signal.get('pipeline', {}).get('trade') else signal.get('state', 'collect')
        candidate = {
            'symbol': signal['symbol'],
            'side': side,
            'stage': stage,
            'score': score,
            'entry_price': entry,
            'stop_price': stop,
            'target_price': target,
            'rr_ratio': rr,
            'execution_target': signal.get('execution_target'),
            'liquidity_context': signal.get('liquidity_context'),
            'notes': signal.get('confirm_source') or signal.get('trigger'),
            'payload': signal,
        }
        return {'accepted': True, 'reason': 'accepted', 'candidate': candidate, 'rr_ratio': rr}

    def build_candidate_from_signal(self, signal: dict) -> dict | None:
        assessment = self.assess_signal(signal)
        return assessment['candidate']

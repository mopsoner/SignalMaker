from datetime import datetime, timezone

from app.services.runtime_settings import load_runtime_settings


MIN_TARGET_DISTANCE_PCT = 0.05
MIN_STOP_DISTANCE_PCT = 0.02


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
            'min_target_distance_pct': MIN_TARGET_DISTANCE_PCT,
            'min_stop_distance_pct': MIN_STOP_DISTANCE_PCT,
        }

    def _watch_reason(self, signal: dict, trade: dict) -> str:
        hierarchy_block_reason = signal.get('hierarchy_block_reason')
        if hierarchy_block_reason:
            return hierarchy_block_reason
        bias = signal.get('bias')
        state = signal.get('state') or 'watch'
        execution_target = (signal.get('execution_target') or {}).get('level')
        entry_context = signal.get('entry_liquidity_context') or {}
        refinement = signal.get('refinement_context_1h') or {}
        exec_trigger = signal.get('execution_trigger') or signal.get('execution_trigger_5m') or {}
        if trade.get('entry') is None:
            if not refinement.get('valid', True):
                return f"watch_missing_1h_setup:{refinement.get('reason', state)}"
            if not exec_trigger.get('valid', True):
                return f"watch_missing_execution_trigger:{exec_trigger.get('trigger', state)}"
            if not signal.get('pipeline', {}).get('confirm'):
                return f"watch_not_confirmed:{state}"
            if execution_target is None:
                return "watch_missing_target_projection"
            if entry_context.get('level') is None:
                return "watch_missing_entry_context"
            return f"watch_missing_entry:{bias or state}"
        return 'watch_unresolved'

    def _as_float(self, value):
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if number <= 0:
            return None
        return number

    def _level_from(self, value):
        if isinstance(value, dict):
            return self._as_float(value.get('level'))
        return self._as_float(value)

    def _distance_pct(self, entry: float, level: float | None) -> float | None:
        if entry <= 0 or level is None:
            return None
        return abs(float(level) - entry) / entry

    def _side_from_signal(self, signal: dict, trade: dict) -> str | None:
        raw_side = (trade.get('side') or '').lower()
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

    def _can_infer_candidate(self, signal: dict) -> bool:
        gate = signal.get('hierarchy_gate') or {}
        exec_trigger = signal.get('execution_trigger') or signal.get('execution_trigger_5m') or {}
        confirmation_model = signal.get('confirmation_model') or {}
        if gate.get('accepted') is True:
            return True
        if signal.get('confirm_blocked_by_hierarchy'):
            return False
        if signal.get('pipeline', {}).get('confirm') and exec_trigger.get('accepted'):
            return True
        return bool(confirmation_model.get('confirmed_by_1h') or confirmation_model.get('confirmed_by_15m'))

    def _add_stop_candidate(self, candidates: list[dict], *, name: str, level, entry: float, side: str, hierarchy_rank: int) -> None:
        level_float = self._level_from(level)
        if level_float is None:
            return
        if side == 'short' and level_float <= entry:
            return
        if side == 'long' and level_float >= entry:
            return
        distance_pct = self._distance_pct(entry, level_float)
        candidates.append({
            'source': name,
            'level': level_float,
            'distance': abs(level_float - entry),
            'distance_pct': distance_pct,
            'hierarchy_rank': hierarchy_rank,
            'valid': bool(distance_pct is not None and distance_pct >= MIN_STOP_DISTANCE_PCT),
            'min_stop_distance_pct': MIN_STOP_DISTANCE_PCT,
            'rejected_reason': None if distance_pct is not None and distance_pct >= MIN_STOP_DISTANCE_PCT else 'stop_distance_below_min_2pct',
        })

    def _infer_stop(self, signal: dict, *, side: str, entry: float) -> tuple[float | None, str | None, list[dict]]:
        candidates: list[dict] = []
        entry_context = signal.get('entry_liquidity_context') or {}
        macro_context = signal.get('macro_liquidity_context') or signal.get('liquidity_context') or {}

        if side == 'short':
            self._add_stop_candidate(candidates, name='entry_liquidity_context', level=entry_context, entry=entry, side=side, hierarchy_rank=10)
            self._add_stop_candidate(candidates, name='external_swing_high', level=signal.get('external_swing_high'), entry=entry, side=side, hierarchy_rank=20)
            self._add_stop_candidate(candidates, name='internal_bear_pivot_high', level=signal.get('internal_bear_pivot_high'), entry=entry, side=side, hierarchy_rank=30)
            self._add_stop_candidate(candidates, name='range_high_1h', level=signal.get('range_high_1h'), entry=entry, side=side, hierarchy_rank=40)
            self._add_stop_candidate(candidates, name='previous_day_high', level=signal.get('previous_day_high'), entry=entry, side=side, hierarchy_rank=50)
            self._add_stop_candidate(candidates, name='macro_liquidity_context', level=macro_context, entry=entry, side=side, hierarchy_rank=60)
        else:
            self._add_stop_candidate(candidates, name='entry_liquidity_context', level=entry_context, entry=entry, side=side, hierarchy_rank=10)
            self._add_stop_candidate(candidates, name='external_swing_low', level=signal.get('external_swing_low'), entry=entry, side=side, hierarchy_rank=20)
            self._add_stop_candidate(candidates, name='internal_bull_pivot_low', level=signal.get('internal_bull_pivot_low'), entry=entry, side=side, hierarchy_rank=30)
            self._add_stop_candidate(candidates, name='range_low_1h', level=signal.get('range_low_1h'), entry=entry, side=side, hierarchy_rank=40)
            self._add_stop_candidate(candidates, name='previous_day_low', level=signal.get('previous_day_low'), entry=entry, side=side, hierarchy_rank=50)
            self._add_stop_candidate(candidates, name='macro_liquidity_context', level=macro_context, entry=entry, side=side, hierarchy_rank=60)

        ordered = sorted(candidates, key=lambda item: (item['hierarchy_rank'], item['distance_pct'] or 999))
        valid_candidates = [item for item in ordered if item.get('valid')]
        if valid_candidates:
            selected = valid_candidates[0]
            return selected['level'], selected['source'], ordered

        return None, 'no_hierarchical_stop_above_min_2pct', ordered

    def _add_target_candidate(self, candidates: list[dict], *, name: str, value, entry: float, side: str, hierarchy_rank: int) -> None:
        level = self._level_from(value)
        if level is None:
            return
        if side == 'short' and level >= entry:
            return
        if side == 'long' and level <= entry:
            return
        distance_pct = self._distance_pct(entry, level)
        candidates.append({
            'source': name,
            'level': level,
            'distance': abs(level - entry),
            'distance_pct': distance_pct,
            'hierarchy_rank': hierarchy_rank,
            'valid': bool(distance_pct is not None and distance_pct >= MIN_TARGET_DISTANCE_PCT),
            'min_target_distance_pct': MIN_TARGET_DISTANCE_PCT,
            'rejected_reason': None if distance_pct is not None and distance_pct >= MIN_TARGET_DISTANCE_PCT else 'target_distance_below_min_5pct',
        })

    def _infer_target(self, signal: dict, *, side: str, entry: float) -> tuple[float | None, str | None, list[dict]]:
        candidates: list[dict] = []
        hierarchy_rank = 10
        ordered_sources = [
            ('execution_target', signal.get('execution_target')),
            ('projected_target', signal.get('projected_target')),
            ('context_selection_debug.selected_target', (signal.get('context_selection_debug') or {}).get('selected_target')),
        ]
        for index, target_candidate in enumerate((signal.get('context_selection_debug') or {}).get('target_candidates') or []):
            ordered_sources.append((f'context_selection_debug.target_candidates[{index}]', target_candidate))

        if side == 'short':
            fallback_keys = ['range_low_1h', 'previous_day_low', 'old_support_shelf', 'previous_week_low', 'range_low_4h', 'major_swing_low_4h']
        else:
            fallback_keys = ['range_high_1h', 'previous_day_high', 'old_resistance_shelf', 'previous_week_high', 'range_high_4h', 'major_swing_high_4h']
        ordered_sources.extend((key, signal.get(key)) for key in fallback_keys)

        seen = set()
        for source, value in ordered_sources:
            level = self._level_from(value)
            if level is not None:
                dedupe_key = (source, round(level, 12))
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
            self._add_target_candidate(candidates, name=source, value=value, entry=entry, side=side, hierarchy_rank=hierarchy_rank)
            hierarchy_rank += 10

        ordered = sorted(candidates, key=lambda item: (item['hierarchy_rank'], item['distance_pct'] or 999))
        valid_candidates = [item for item in ordered if item.get('valid')]
        if valid_candidates:
            selected = valid_candidates[0]
            return selected['level'], selected['source'], ordered
        return None, 'no_hierarchical_target_above_min_5pct', ordered

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
            return {'side': side}, self._watch_reason(signal, trade)

        stop = self._as_float(trade.get('stop'))
        stop_source = trade.get('stop_source') or 'trade.stop'
        stop_candidates: list[dict] = []
        if stop is None:
            stop, stop_source, stop_candidates = self._infer_stop(signal, side=side, entry=entry)
        if stop is None:
            return {'side': side, 'entry': entry, 'stop_candidates': stop_candidates}, stop_source or 'missing_stop'

        target = self._as_float(trade.get('target'))
        target_source = trade.get('target_source') or 'trade.target'
        target_candidates: list[dict] = []
        if target is None:
            target, target_source, target_candidates = self._infer_target(signal, side=side, entry=entry)
        if target is None:
            return {'side': side, 'entry': entry, 'stop': stop, 'stop_candidates': stop_candidates, 'target_candidates': target_candidates}, target_source or 'missing_target'

        stop_distance_pct = self._distance_pct(entry, stop)
        target_distance_pct = self._distance_pct(entry, target)
        if stop_distance_pct is None or stop_distance_pct < MIN_STOP_DISTANCE_PCT:
            return {
                'side': side,
                'entry': entry,
                'stop': stop,
                'target': target,
                'stop_distance_pct': stop_distance_pct,
                'min_stop_distance_pct': MIN_STOP_DISTANCE_PCT,
                'stop_candidates': stop_candidates,
            }, 'stop_distance_below_min_2pct'
        if target_distance_pct is None or target_distance_pct < MIN_TARGET_DISTANCE_PCT:
            return {
                'side': side,
                'entry': entry,
                'stop': stop,
                'target': target,
                'target_distance_pct': target_distance_pct,
                'min_target_distance_pct': MIN_TARGET_DISTANCE_PCT,
                'target_candidates': target_candidates,
            }, 'target_distance_below_min_5pct'

        if side == 'short':
            if stop <= entry:
                return {'side': side, 'entry': entry, 'stop': stop, 'target': target}, 'invalid_short_stop'
            if target >= entry:
                return {'side': side, 'entry': entry, 'stop': stop, 'target': target}, 'invalid_short_target'
        else:
            if stop >= entry:
                return {'side': side, 'entry': entry, 'stop': stop, 'target': target}, 'invalid_long_stop'
            if target <= entry:
                return {'side': side, 'entry': entry, 'stop': stop, 'target': target}, 'invalid_long_target'

        resolved = {
            'status': 'planned',
            'side': side,
            'entry': entry,
            'stop': stop,
            'target': target,
            'entry_source': entry_source,
            'stop_source': stop_source,
            'target_source': target_source,
            'stop_distance_pct': stop_distance_pct,
            'target_distance_pct': target_distance_pct,
            'min_stop_distance_pct': MIN_STOP_DISTANCE_PCT,
            'min_target_distance_pct': MIN_TARGET_DISTANCE_PCT,
            'inferred_by': 'planner_from_accepted_signal_v1' if trade.get('entry') is None else 'signal_trade_object',
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

        side = resolved_trade['side']
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
        signal['planner_trade_plan'] = {
            'model': 'accepted_signal_inference_v1',
            'side': side,
            'entry': entry,
            'stop': stop,
            'target': target,
            'entry_source': resolved_trade.get('entry_source'),
            'stop_source': resolved_trade.get('stop_source'),
            'target_source': resolved_trade.get('target_source'),
            'stop_distance_pct': resolved_trade.get('stop_distance_pct'),
            'target_distance_pct': resolved_trade.get('target_distance_pct'),
            'min_stop_distance_pct': MIN_STOP_DISTANCE_PCT,
            'min_target_distance_pct': MIN_TARGET_DISTANCE_PCT,
            'rr_ratio': rr,
        }
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

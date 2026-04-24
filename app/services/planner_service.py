from datetime import datetime, timezone

from app.services.runtime_settings import load_runtime_settings


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
        }

    def _market_data_invalid_reason(self, signal: dict) -> str | None:
        volume_debug = signal.get('volume_debug', {}) or {}
        market_quality_debug = signal.get('market_quality_debug', {}) or {}
        candle_quality = signal.get('candle_quality_5m', {}) or {}

        volume_last = float(volume_debug.get('last') or 0.0)
        volume_average = float(volume_debug.get('average') or 0.0)
        avg_range_pct = float(market_quality_debug.get('avg_range_pct') or 0.0)
        candle_count = int(candle_quality.get('count') or 0)

        if candle_count > 0 and volume_last <= 0 and volume_average <= 0 and avg_range_pct <= 0:
            return 'invalid_market_data:zero_volume_zero_range'
        if candle_count > 0 and volume_average <= 0:
            return 'invalid_market_data:zero_average_volume'
        if candle_count > 0 and avg_range_pct <= 0:
            return 'invalid_market_data:zero_average_range'
        return None

    def _watch_reason(self, signal: dict, trade: dict) -> str:
        invalid_data_reason = self._market_data_invalid_reason(signal)
        if invalid_data_reason:
            return invalid_data_reason

        hierarchy_block_reason = signal.get('hierarchy_block_reason')
        if hierarchy_block_reason:
            return hierarchy_block_reason
        bias = signal.get('bias')
        state = signal.get('state') or 'watch'
        execution_target = (signal.get('execution_target') or {}).get('level')
        entry_context = signal.get('entry_liquidity_context') or {}
        macro_window = signal.get('macro_window_4h') or {}
        refinement = signal.get('refinement_context_1h') or {}
        exec_trigger = signal.get('execution_trigger_5m') or {}
        if trade.get('entry') is None:
            if not macro_window.get('valid', True):
                side = macro_window.get('side') or 'neutral'
                return f"watch_outside_4h_window:{side}"
            if not refinement.get('valid', True):
                return f"watch_missing_1h_setup:{refinement.get('reason', state)}"
            if not exec_trigger.get('valid', True):
                return f"watch_missing_5m_trigger:{exec_trigger.get('trigger', state)}"
            if not signal.get('pipeline', {}).get('confirm'):
                return f"watch_not_confirmed:{state}"
            if execution_target is None:
                return "watch_missing_target_projection"
            if entry_context.get('level') is None:
                return "watch_missing_entry_context"
            return f"watch_missing_entry:{bias or state}"
        return 'watch_unresolved'

    def assess_signal(self, signal: dict) -> dict:
        runtime = load_runtime_settings()
        strategy = runtime['strategy']
        trade = signal.get('trade', {}) or {}
        side = trade.get('side')
        entry = trade.get('entry')
        stop = trade.get('stop')
        target = trade.get('target')
        score = float(signal.get('score', 0.0))

        invalid_data_reason = self._market_data_invalid_reason(signal)
        if invalid_data_reason:
            return {'accepted': False, 'reason': invalid_data_reason, 'candidate': None}

        if not side or side == 'none':
            return {'accepted': False, 'reason': self._watch_reason(signal, trade), 'candidate': None}
        if entry is None:
            return {'accepted': False, 'reason': self._watch_reason(signal, trade), 'candidate': None}
        if stop is None:
            return {'accepted': False, 'reason': 'missing_stop', 'candidate': None}
        if target is None:
            return {'accepted': False, 'reason': 'missing_target', 'candidate': None}

        risk = abs(entry - stop)
        reward = abs(target - entry)
        rr = reward / risk if risk > 0 else None
        if risk <= 0:
            return {'accepted': False, 'reason': 'invalid_risk', 'candidate': None, 'rr_ratio': rr}
        if score < strategy['planner_min_score']:
            return {'accepted': False, 'reason': 'low_score', 'candidate': None, 'rr_ratio': rr}
        if rr is None or rr < strategy['planner_min_rr']:
            return {'accepted': False, 'reason': 'low_rr', 'candidate': None, 'rr_ratio': rr}

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

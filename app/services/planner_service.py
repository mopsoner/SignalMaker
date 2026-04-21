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

    def _watch_reason(self, signal: dict, trade: dict) -> str:
        bias = signal.get('bias')
        state = signal.get('state') or 'watch'
        execution_target = (signal.get('execution_target') or {}).get('level')
        entry_context = signal.get('entry_liquidity_context') or {}
        if trade.get('entry') is None:
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

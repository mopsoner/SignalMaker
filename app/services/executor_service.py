from sqlalchemy.orm import Session

from app.services.fill_service import FillService
from app.services.order_service import OrderService
from app.services.position_service import PositionService
from app.services.runtime_settings import load_runtime_settings
from app.services.trade_candidate_service import TradeCandidateService


class ExecutorService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.orders = OrderService(db)
        self.fills = FillService(db)
        self.positions = PositionService(db)
        self.candidates = TradeCandidateService(db)

    def _is_short_side(self, side: str | None) -> bool:
        return (side or '').lower() in {'short', 'sell', 'bear'}

    def _level_from_payload(self, payload: dict, key: str):
        value = payload.get(key)
        if isinstance(value, dict):
            return value.get('level')
        return value

    def _current_price_for_candidate(self, candidate, *, requested_mode: str) -> float:
        return float(candidate.entry_price)

    def _price_between_stop_and_target(self, candidate, target_price: float, mark_price: float) -> bool:
        if candidate.stop_price is None or target_price is None:
            return False
        stop = float(candidate.stop_price)
        target = float(target_price)
        mark = float(mark_price)
        if self._is_short_side(candidate.side):
            return target < mark < stop
        return stop < mark < target

    def _add_target_candidate(self, candidates: list[dict], *, name: str, level, source: str, rank: int, entry: float, is_short: bool) -> None:
        if level is None:
            return
        try:
            level_float = float(level)
        except (TypeError, ValueError):
            return
        if level_float <= 0:
            return
        if is_short and level_float >= entry:
            return
        if not is_short and level_float <= entry:
            return
        distance_pct = abs(entry - level_float) / entry if entry else None
        for candidate in candidates:
            if abs(float(candidate['level']) - level_float) / max(entry, 1e-12) < 0.00025:
                candidate['sources'].append(source)
                candidate['rank'] = min(candidate['rank'], rank)
                candidate['name'] = candidate['name'] if candidate['rank'] <= rank else name
                return
        candidates.append(
            {
                'name': name,
                'level': level_float,
                'source': source,
                'sources': [source],
                'rank': rank,
                'distance_pct': distance_pct,
            }
        )

    def _hierarchical_target_plan(self, candidate, *, fill_price: float | None = None) -> dict:
        entry = fill_price if fill_price is not None else candidate.entry_price
        stop = candidate.stop_price
        raw_target = candidate.target_price
        payload = candidate.payload or {}
        is_short = self._is_short_side(candidate.side)

        if entry is None or raw_target is None:
            return {
                'target_price': raw_target,
                'raw_target_price': raw_target,
                'target_model': 'raw_missing_entry_or_target',
                'target_candidates': [],
            }

        entry = float(entry)
        raw_target = float(raw_target)
        risk = abs(entry - float(stop)) if stop is not None else None
        runtime = load_runtime_settings(getattr(self, 'db', None))
        planner_min_rr = float(runtime['strategy'].get('planner_min_rr', 0.8))
        candidate_rr = float(candidate.rr_ratio or 0.0)
        min_reward_ratio = max(0.75, planner_min_rr, candidate_rr)
        min_reward = risk * min_reward_ratio if risk and risk > 0 else 0.0

        candidates: list[dict] = []
        liquidity_context = candidate.liquidity_context or payload.get('liquidity_context') or payload.get('macro_liquidity_context') or {}
        execution_target = candidate.execution_target or payload.get('execution_target') or payload.get('projected_target') or {}
        context_debug = payload.get('context_selection_debug') or {}
        selected_target = context_debug.get('selected_target') or {}
        target_candidates = context_debug.get('target_candidates') or []

        # 1) First TP should often be the swept/reclaimed macro context level itself.
        # Example short after UTAD above range high: entry 0.489, context 0.467, macro target 0.454.
        self._add_target_candidate(
            candidates,
            name=liquidity_context.get('type') or 'macro_context_level',
            level=liquidity_context.get('level'),
            source='macro_context_level',
            rank=10,
            entry=entry,
            is_short=is_short,
        )

        # 2) Then use explicit ranked target candidates from the strategy, but keep nearest hierarchy first.
        for index, target_candidate in enumerate(target_candidates):
            self._add_target_candidate(
                candidates,
                name=target_candidate.get('type') or target_candidate.get('source') or f'ranked_target_{index}',
                level=target_candidate.get('level'),
                source=target_candidate.get('source') or f'context_selection_debug.target_candidates[{index}]',
                rank=20 + index,
                entry=entry,
                is_short=is_short,
            )
        self._add_target_candidate(
            candidates,
            name=selected_target.get('type') or 'selected_ranked_target',
            level=selected_target.get('level'),
            source='context_selection_debug.selected_target',
            rank=25,
            entry=entry,
            is_short=is_short,
        )
        self._add_target_candidate(
            candidates,
            name=execution_target.get('type') or 'execution_target',
            level=execution_target.get('level'),
            source='candidate.execution_target',
            rank=30,
            entry=entry,
            is_short=is_short,
        )

        # 3) Fallback hierarchy from the current payload. Pick the next nearby level, not the far macro extreme.
        if is_short:
            fallback_keys = [
                ('recent_low_1h', 'recent_low_1h', 40),
                ('range_low_1h', 'range_low_1h', 45),
                ('previous_day_low', 'previous_day_low', 50),
                ('old_support_shelf', 'old_support_shelf', 55),
                ('previous_week_low', 'previous_week_low', 60),
                ('range_low_4h', 'range_low_4h', 70),
                ('major_swing_low_4h', 'major_swing_low_4h', 90),
            ]
        else:
            fallback_keys = [
                ('recent_high_1h', 'recent_high_1h', 40),
                ('range_high_1h', 'range_high_1h', 45),
                ('previous_day_high', 'previous_day_high', 50),
                ('old_resistance_shelf', 'old_resistance_shelf', 55),
                ('previous_week_high', 'previous_week_high', 60),
                ('range_high_4h', 'range_high_4h', 70),
                ('major_swing_high_4h', 'major_swing_high_4h', 90),
            ]
        for name, key, rank in fallback_keys:
            self._add_target_candidate(
                candidates,
                name=name,
                level=self._level_from_payload(payload, key),
                source=key,
                rank=rank,
                entry=entry,
                is_short=is_short,
            )

        # 4) Prefer the closest valid level in the hierarchy. Avoid micro targets below the local SL risk when possible.
        viable = []
        for item in candidates:
            reward = abs(entry - float(item['level']))
            item = {**item, 'reward_price_distance': reward}
            if min_reward <= 0 or reward >= min_reward:
                viable.append(item)
        pool = viable or [{**item, 'reward_price_distance': abs(entry - float(item['level']))} for item in candidates]
        if pool:
            selected = sorted(pool, key=lambda item: (abs(entry - float(item['level'])), item['rank']))[0]
            target_price = float(selected['level'])
            rr = abs(target_price - entry) / risk if risk and risk > 0 else None
            return {
                'target_price': target_price,
                'raw_target_price': raw_target,
                'target_model': 'hierarchical_position_target_v2_next_rr_target',
                'selected_position_target': selected,
                'target_candidates': sorted(candidates, key=lambda item: (abs(entry - float(item['level'])), item['rank'])),
                'position_rr': rr,
                'risk_price_distance': risk,
                'min_reward_ratio': min_reward_ratio,
                'min_reward_price_distance': min_reward,
            }

        return {
            'target_price': raw_target,
            'raw_target_price': raw_target,
            'target_model': 'raw_no_hierarchical_target',
            'target_candidates': [],
            'risk_price_distance': risk,
        }

    def _execute_paper_candidate(self, candidate, quantity: float) -> dict:
        target_plan = self._hierarchical_target_plan(candidate)
        target_price = target_plan['target_price']
        position = self.positions.create_position(
            symbol=candidate.symbol,
            side=candidate.side,
            quantity=quantity,
            entry_price=candidate.entry_price,
            mark_price=candidate.entry_price,
            stop_price=candidate.stop_price,
            target_price=target_price,
            meta={"candidate_id": candidate.candidate_id, "mode": "paper", **target_plan},
        )
        order = self.orders.create_order(
            candidate_id=candidate.candidate_id,
            position_id=position.position_id,
            symbol=candidate.symbol,
            side=candidate.side,
            order_type="market",
            quantity=quantity,
            requested_price=candidate.entry_price,
            filled_price=candidate.entry_price,
            status="filled",
            meta={"mode": "paper", **target_plan},
        )
        fill = self.fills.create_fill(order_id=order.order_id, position_id=position.position_id, symbol=candidate.symbol, side=candidate.side, quantity=quantity, price=candidate.entry_price)
        self.candidates.mark_executed(candidate.candidate_id)
        return {"candidate_id": candidate.candidate_id, "position_id": position.position_id, "order_id": order.order_id, "fill_id": fill.fill_id, "mode": "paper", "target_price": target_price, "raw_target_price": target_plan.get('raw_target_price')}

    def _execute_live_candidate(self, candidate, quantity: float) -> dict:
        raise RuntimeError('Live exchange execution has been removed from SignalMaker main; use the Raspberry Executor for real orders')

    def execute_open_candidates(self, limit: int = 100, quantity: float = 1.0, mode: str = 'paper') -> dict:
        executed = []
        skipped = []
        requested_mode = (mode or 'paper').lower()
        for candidate in self.candidates.get_open_candidates(limit=limit):
            if candidate.entry_price is None:
                skipped.append({'candidate_id': candidate.candidate_id, 'reason': 'missing_entry_price'})
                continue
            try:
                target_plan = self._hierarchical_target_plan(candidate)
                target_price = target_plan['target_price']
                mark_price = self._current_price_for_candidate(candidate, requested_mode=requested_mode)
                if not self._price_between_stop_and_target(candidate, target_price, mark_price):
                    skipped.append({
                        'candidate_id': candidate.candidate_id,
                        'reason': 'current_price_outside_stop_target_range',
                        'mark_price': mark_price,
                        'stop_price': candidate.stop_price,
                        'target_price': target_price,
                    })
                    continue
                if requested_mode == 'live':
                    result = self._execute_live_candidate(candidate, quantity)
                else:
                    result = self._execute_paper_candidate(candidate, quantity)
                executed.append(result)
            except Exception as exc:
                skipped.append({'candidate_id': candidate.candidate_id, 'reason': str(exc)})
        return {'mode': requested_mode, 'executed': executed, 'skipped': skipped}

    def reconcile_live_positions(self) -> dict:
        return {
            'enabled': False,
            'checked': 0,
            'closed': [],
            'updated': [],
            'reason': 'Live exchange reconciliation is handled by the Raspberry Executor',
        }

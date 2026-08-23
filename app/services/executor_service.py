from typing import Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import settings

from app.services.fill_service import FillService
from app.services.order_service import OrderService
from app.services.position_service import PositionService
from app.services.runtime_settings import load_runtime_settings
from app.services.trade_candidate_service import TradeCandidateService
from app.services.execution.kraken_execution_service import KrakenExecutionService
from app.services.execution.live_configuration import assert_wyckoff_live_configuration
from app.models.candidate_execution import CandidateExecution
from app.models.order import Order

ExecutionMode = Literal["paper", "live"]


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
        planner_min_rr = float(runtime['strategy']['planner_min_rr'])
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
            meta={"candidate_id": candidate.candidate_id, "execution_mode": "paper", **target_plan},
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
            meta={"execution_mode": "paper", **target_plan},
        )
        fill = self.fills.create_fill(order_id=order.order_id, position_id=position.position_id, symbol=candidate.symbol, side=candidate.side, quantity=quantity, price=candidate.entry_price)
        return {"candidate_id": candidate.candidate_id, "position_id": position.position_id, "order_id": order.order_id, "fill_id": fill.fill_id, "mode": "paper", "target_price": target_price, "raw_target_price": target_plan.get('raw_target_price')}

    def _execute_live_candidate(self, candidate, quantity: float) -> dict:
        target_plan = self._hierarchical_target_plan(candidate)
        runtime = load_runtime_settings(self.db)
        live = runtime.get("live", {})
        if live.get("live_require_tp_sl", True) and (
            candidate.stop_price is None or target_plan.get("target_price") is None
        ):
            raise ValueError("live candidate requires stop-loss and take-profit levels")

        mode = settings.wyckoff_live_mode.lower()
        execution = KrakenExecutionService(self.db)
        state = self.db.scalar(
            select(CandidateExecution).where(
                CandidateExecution.candidate_id == candidate.candidate_id,
                CandidateExecution.execution_mode == "live",
            )
        )
        exchange_order = None
        if state is not None and state.entry_order_id:
            entry_order_id = state.entry_order_id
        elif self._is_short_side(candidate.side):
            if mode == "spot":
                raise ValueError("bearish Wyckoff candidates require margin mode")
            exchange_order = execution.sell_market(
                candidate.symbol,
                quantity=quantity,
                mode=mode,
                intent="open_short",
            )
        else:
            raw_notional = float(candidate.entry_price) * quantity
            min_notional = float(live["live_min_total_notional_per_trade"])
            max_notional = float(live["live_max_notional_per_trade"])
            if min_notional <= 0 or min_notional > max_notional:
                raise ValueError("invalid live total notional range")
            requested_notional = min(max(raw_notional, min_notional), max_notional)
            exchange_order = execution.buy_market(
                candidate.symbol,
                total_notional=requested_notional,
                mode=mode,
            )
        if exchange_order is not None:
            entry_order_id = str(exchange_order["order_id"])
            if state is not None:
                state.entry_order_id = entry_order_id
                state.entry_order_status = str(exchange_order.get("status") or "pending")
                self.db.commit()

        entry = execution.get_order(candidate.symbol, entry_order_id, mode=mode)
        entry_status = str(entry.get("status") or "unknown").lower()
        executed_quantity = float(entry.get("executed_quantity") or 0)
        average_price = float(entry.get("average_price") or 0)
        persisted_entry = self.db.get(Order, entry_order_id)
        if persisted_entry is not None:
            persisted_entry.status = entry_status
            persisted_entry.filled_price = average_price or None
            persisted_entry.quantity = executed_quantity or persisted_entry.quantity
        if state is not None:
            state.entry_order_status = entry_status
        self.db.commit()
        if entry_status != "filled":
            return {
                "candidate_id": candidate.candidate_id,
                "mode": "live",
                "exchange": "kraken",
                "exchange_order": exchange_order or entry,
                "entry_order_id": entry_order_id,
                "entry_order_status": entry_status,
                "executed_quantity": executed_quantity,
                "protection_installed": False,
                "pending": True,
                "target_price": target_plan.get("target_price"),
                "stop_price": candidate.stop_price,
            }
        if executed_quantity <= 0 or average_price <= 0:
            raise RuntimeError("Kraken reported a filled entry without executed quantity and average price")

        exit_side = "buy" if self._is_short_side(candidate.side) else "sell"
        leverage = (exchange_order or entry).get("effective_leverage") or entry.get("leverage")
        if state is not None and state.take_profit_order_id:
            take_profit = {"order_id": state.take_profit_order_id, "status": state.take_profit_order_status}
        else:
            take_profit = execution.place_take_profit(
                candidate.symbol, exit_side, executed_quantity, float(target_plan["target_price"]),
                mode=mode, leverage=leverage,
            )
            if state is not None:
                state.take_profit_order_id = str(take_profit["order_id"])
                state.take_profit_order_status = str(take_profit.get("status") or "pending")
                self.db.commit()
        position = self.positions.create_position(
            symbol=candidate.symbol,
            side=candidate.side,
            quantity=executed_quantity,
            entry_price=average_price,
            mark_price=average_price,
            stop_price=candidate.stop_price,
            target_price=target_plan["target_price"],
            meta={"candidate_id": candidate.candidate_id, "execution_mode": "live", **target_plan},
        )
        position.entry_order_id = entry_order_id
        position.entry_order_status = entry_status
        position.take_profit_order_id = str(take_profit["order_id"])
        position.take_profit_order_status = str(take_profit.get("status") or "pending")
        for order_id in (entry_order_id, position.take_profit_order_id):
            row = self.db.get(Order, order_id)
            if row is not None:
                row.candidate_id = candidate.candidate_id
                row.position_id = position.position_id
        if state is not None:
            state.take_profit_order_id = position.take_profit_order_id
            state.take_profit_order_status = position.take_profit_order_status
        self.db.commit()
        return {
            "candidate_id": candidate.candidate_id,
            "mode": "live",
            "exchange": "kraken",
            "exchange_order": exchange_order,
            "entry_order_id": entry_order_id,
            "take_profit_order_id": position.take_profit_order_id,
            "position_id": position.position_id,
            "executed_quantity": executed_quantity,
            "average_price": average_price,
            "protection_installed": True,
            "pending": False,
            "target_price": target_plan.get("target_price"),
            "stop_price": candidate.stop_price,
        }

    def execute_open_candidates(
        self, limit: int = 100, quantity: float = 1.0, mode: ExecutionMode = "paper"
    ) -> dict:
        executed = []
        skipped = []
        requested_mode = mode
        if requested_mode not in {'paper', 'live'}:
            raise ValueError('execution mode must be paper or live')
        if requested_mode == "live":
            # Validate before claiming candidates or constructing a Kraken client.
            assert_wyckoff_live_configuration(settings)
        candidates = []
        if requested_mode == "live":
            candidates = self.candidates.get_pending_candidates(execution_mode=requested_mode, limit=limit)
        remaining = max(0, limit - len(candidates))
        if remaining:
            candidates.extend(
                self.candidates.claim_open_candidates(execution_mode=requested_mode, limit=remaining)
            )
        for candidate in candidates:
            if candidate.entry_price is None:
                skipped.append({'candidate_id': candidate.candidate_id, 'reason': 'missing_entry_price'})
                self.candidates.release_claim(candidate.candidate_id, execution_mode=requested_mode)
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
                    self.candidates.release_claim(candidate.candidate_id, execution_mode=requested_mode)
                    continue
                if requested_mode == 'live':
                    result = self._execute_live_candidate(candidate, quantity)
                else:
                    result = self._execute_paper_candidate(candidate, quantity)
                if requested_mode != "live" or result.get("protection_installed"):
                    self.candidates.finish_execution(candidate.candidate_id, execution_mode=requested_mode)
                executed.append(result)
            except Exception as exc:
                if requested_mode == "live":
                    self.candidates.record_pending_error(
                        candidate.candidate_id, execution_mode=requested_mode, error=str(exc)
                    )
                else:
                    self.candidates.finish_execution(
                        candidate.candidate_id, execution_mode=requested_mode, error=str(exc)
                    )
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

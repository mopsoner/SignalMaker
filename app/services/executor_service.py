from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.order import Order
from app.models.trade_candidate import TradeCandidate
from app.schemas.momentum import SUPPORTED_MOMENTUM_EXECUTOR_ACTIONS

from app.services.exchange_adapter import create_execution_adapter
from app.services.fill_service import FillService
from app.services.order_service import OrderService
from app.services.position_service import PositionService
from app.services.risk_service import RiskService
from app.services.runtime_settings import load_runtime_settings
from app.services.trade_candidate_service import TradeCandidateService
from raspberry_executor.kraken_margin_client import KrakenMarginClient
from raspberry_executor.kraken_symbol_rules import KrakenSymbolRules
from raspberry_executor.margin_order_manager import MarginOrderManager
from raspberry_executor.margin_settings import margin_enabled, margin_leverage_attempts, margin_multiplier
from raspberry_executor.spot_order_manager import SpotOrderManager


class ExecutorService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.orders = OrderService(db)
        self.fills = FillService(db)
        self.positions = PositionService(db)
        self.candidates = TradeCandidateService(db)
        self.risk = RiskService(db)
        self.exchange = create_execution_adapter(db)
        self.kraken = self.exchange  # backward-compatible test/extension hook

    def _runtime_section(self, section: str) -> dict:
        values = load_runtime_settings(self.db).get(section, {})
        return values if isinstance(values, dict) else {}

    def _runtime_csv(self, section: str, key: str) -> list[str]:
        return [item.strip() for item in str(self._runtime_section(section).get(key, '')).split(',') if item.strip()]

    def _is_short_side(self, side: str | None) -> bool:
        return (side or '').lower() in {'short', 'sell', 'bear'}

    def _current_price_for_candidate(self, candidate, *, requested_mode: str) -> float:
        if requested_mode == 'live':
            return self.kraken.current_price(candidate.symbol)
        return float(candidate.entry_price)

    def _price_before_target(self, candidate, mark_price: float) -> bool:
        if candidate.target_price is None:
            return False
        target = float(candidate.target_price)
        mark = float(mark_price)
        if self._is_short_side(candidate.side):
            return target < mark
        return mark < target

    def _execute_paper_candidate(self, candidate, quantity: float) -> dict:
        position = self.positions.create_position(symbol=candidate.symbol, side=candidate.side, quantity=quantity, entry_price=candidate.entry_price, mark_price=candidate.entry_price, stop_price=candidate.stop_price, target_price=candidate.target_price, meta={"candidate_id": candidate.candidate_id, "mode": "paper"})
        order = self.orders.create_order(candidate_id=candidate.candidate_id, position_id=position.position_id, symbol=candidate.symbol, side=candidate.side, order_type="market", quantity=quantity, requested_price=candidate.entry_price, filled_price=candidate.entry_price, status="filled", meta={"mode": "paper"})
        fill = self.fills.create_fill(order_id=order.order_id, position_id=position.position_id, symbol=candidate.symbol, side=candidate.side, quantity=quantity, price=candidate.entry_price)
        self.candidates.mark_executed(candidate.candidate_id)
        return {"candidate_id": candidate.candidate_id, "position_id": position.position_id, "order_id": order.order_id, "fill_id": fill.fill_id, "mode": "paper"}

    def _kraken_margin_order_manager(self) -> MarginOrderManager:
        active = self.kraken
        if not hasattr(active, 'client'):
            raise RuntimeError('live momentum margin execution requires a Kraken client adapter')
        rules = KrakenSymbolRules(str(self._runtime_section('kraken').get('kraken_base_url')), quote_assets=self._runtime_csv('market_data', 'kraken_quote_assets'))
        margin = KrakenMarginClient(active.client, isolated=False, dry_run=active.client.dry_run, leverage=margin_multiplier())
        return MarginOrderManager(active.client, margin, rules)

    def _execute_live_momentum_leveraged_buy(self, candidate, quantity: float) -> dict:
        active = self.kraken
        exchange_name = getattr(active, 'exchange_name', getattr(self.exchange, 'exchange_name', 'kraken'))
        if not active.is_configured():
            raise RuntimeError(f'{exchange_name} live trading requested but API credentials are missing')
        normalized = active.normalize_order(candidate.symbol, quantity=quantity, target_price=candidate.target_price, stop_price=None)
        manager = self._kraken_margin_order_manager()
        quote_amount = float(quantity) * float(candidate.entry_price or normalized['mark_price'])
        attempts = []
        margin_result = None
        used_leverage = None
        for leverage in margin_leverage_attempts():
            try:
                result = manager.open_long_with_margin_take_profit(symbol=candidate.symbol, quote_amount=quote_amount, target_price=float(normalized['target_price']), leverage=leverage)
                if result.get('tp_error'):
                    raise RuntimeError(result['tp_error'])
                margin_result = result
                used_leverage = leverage
                break
            except Exception as exc:
                attempts.append({'leverage': leverage, 'error': str(exc)})
        if margin_result is None:
            raise RuntimeError(f'momentum_margin_buy_failed:{attempts}')

        entry_resp = margin_result.get('entry_payload') or {}
        tp_resp = margin_result.get('tp_payload') or {}
        avg_fill = float(margin_result['entry_price'])
        filled_qty = float(margin_result['quantity'])
        position = self.positions.create_position(
            symbol=candidate.symbol, side=candidate.side, quantity=filled_qty, entry_price=avg_fill, mark_price=avg_fill, stop_price=normalized.get('stop_price'), target_price=normalized.get('target_price'),
            meta={'candidate_id': candidate.candidate_id, 'mode': 'cross_margin', 'margin_isolated': False, 'symbol': candidate.symbol, 'exchange': exchange_name, 'entry_exchange_order_id': margin_result.get('entry_order_id') or entry_resp.get('orderId'), 'tp_exchange_order_id': margin_result.get('tp_order_id') or tp_resp.get('orderId'), 'leverage': used_leverage, 'margin_attempts': attempts},
        )
        entry_order = self.orders.create_order(candidate_id=candidate.candidate_id, position_id=position.position_id, symbol=candidate.symbol, side='buy', order_type='market', quantity=filled_qty, requested_price=candidate.entry_price, filled_price=avg_fill, status=str(entry_resp.get('status', margin_result.get('entry_confirm_status', 'filled'))).lower(), meta={'mode': 'cross_margin', 'exchange': exchange_name, 'exchange_payload': entry_resp, 'exchange_order_id': margin_result.get('entry_order_id') or entry_resp.get('orderId'), 'leverage': used_leverage, 'margin_attempts': attempts})
        fill = self.fills.create_fill(order_id=entry_order.order_id, position_id=position.position_id, symbol=candidate.symbol, side='buy', quantity=filled_qty, price=avg_fill)
        tp_local = self.orders.create_order(candidate_id=candidate.candidate_id, position_id=position.position_id, symbol=candidate.symbol, side='sell', order_type='take_profit', quantity=filled_qty, requested_price=normalized['target_price'], filled_price=None, status=str(tp_resp.get('status', 'open')).lower(), meta={'mode': 'cross_margin', 'exchange': exchange_name, 'exchange_payload': tp_resp, 'exchange_order_id': margin_result.get('tp_order_id') or tp_resp.get('orderId')})
        position.meta = {**(position.meta or {}), 'tp_local_order_id': tp_local.order_id, 'exit_strategy': 'take_profit_only'}
        self.db.commit()
        self.db.refresh(position)
        self.candidates.mark_executed(candidate.candidate_id)
        return {'candidate_id': candidate.candidate_id, 'position_id': position.position_id, 'entry_order_id': entry_order.order_id, 'fill_id': fill.fill_id, 'tp_order_id': tp_local.order_id, 'mode': 'cross_margin', 'exchange': exchange_name, 'exchange_entry_order_id': margin_result.get('entry_order_id') or entry_resp.get('orderId'), 'exchange_tp_order_id': margin_result.get('tp_order_id') or tp_resp.get('orderId'), 'leverage': used_leverage, 'margin_attempts': attempts}

    def _execute_live_candidate(self, candidate, quantity: float) -> dict:
        self.risk.validate_live_candidate(symbol=candidate.symbol, side=candidate.side, entry_price=candidate.entry_price, stop_price=candidate.stop_price, target_price=candidate.target_price, quantity=quantity)
        active = self.kraken
        exchange_name = getattr(active, 'exchange_name', getattr(self.exchange, 'exchange_name', 'kraken'))
        if not active.is_configured():
            raise RuntimeError(f'{exchange_name} live trading requested but API credentials are missing')
        live_runtime = self._runtime_section('live')
        if candidate.side != 'long' and not live_runtime.get('live_spot_allow_shorts'):
            raise RuntimeError('Live short execution is not supported in current spot mode')

        normalized = active.normalize_order(candidate.symbol, quantity=quantity, target_price=candidate.target_price, stop_price=None)
        execution_mode = 'spot'
        margin_error = None
        margin_attempts = []
        used_leverage = None
        if candidate.side == 'long' and margin_enabled() and hasattr(active, 'client'):
            rules = KrakenSymbolRules(str(self._runtime_section('kraken').get('kraken_base_url')), quote_assets=self._runtime_csv('market_data', 'kraken_quote_assets'))
            margin = KrakenMarginClient(active.client, isolated=False, dry_run=active.client.dry_run, leverage=margin_multiplier())
            margin_manager = MarginOrderManager(active.client, margin, rules)
            quote_amount = float(quantity) * float(candidate.entry_price or normalized['mark_price'])
            for leverage in margin_leverage_attempts():
                try:
                    margin_result = margin_manager.open_long_with_margin_take_profit(symbol=candidate.symbol, quote_amount=quote_amount, target_price=float(normalized['target_price']), leverage=leverage)
                    if margin_result.get('tp_error'):
                        raise RuntimeError(margin_result['tp_error'])
                    entry_resp = margin_result.get('entry_payload') or {}
                    tp_resp = margin_result.get('tp_payload') or {}
                    avg_fill = float(margin_result['entry_price'])
                    filled_qty = float(margin_result['quantity'])
                    execution_mode = 'cross_margin'
                    used_leverage = leverage
                    break
                except Exception as exc:
                    margin_attempts.append({'leverage': leverage, 'error': str(exc)})
            if execution_mode != 'cross_margin' and margin_attempts:
                margin_error = f'margin attempts failed: {margin_attempts}'
        if execution_mode == 'spot':
            try:
                if hasattr(active, 'client'):
                    rules = KrakenSymbolRules(str(self._runtime_section('kraken').get('kraken_base_url')), quote_assets=self._runtime_csv('market_data', 'kraken_quote_assets'))
                    spot_result = SpotOrderManager(active.client, rules).open_long_with_take_profit(symbol=candidate.symbol, quote_amount=float(quantity) * float(candidate.entry_price or normalized['mark_price']), target_price=float(normalized['target_price']))
                    entry_resp = spot_result.get('entry_payload') or {}
                    tp_resp = spot_result.get('tp_payload') or {}
                    avg_fill = float(spot_result['entry_price'])
                    filled_qty = float(spot_result['quantity'])
                elif hasattr(active, 'place_market_entry'):
                    entry_resp = active.place_market_entry(candidate.symbol, candidate.side, normalized['quantity'])
                    avg_fill = active.average_fill_price(entry_resp, fallback=candidate.entry_price or normalized['mark_price']) or candidate.entry_price or normalized['mark_price']
                    filled_qty = float(entry_resp.get('executedQty') or normalized['quantity'])
                    tp_resp = active.place_exit_limit(candidate.symbol, candidate.side, quantity=filled_qty, price=normalized['target_price'])
                else:
                    entry_resp = active.place_market_buy(candidate.symbol, normalized['quantity'])
                    avg_fill = active.average_fill_price(entry_resp) or candidate.entry_price or normalized['mark_price']
                    filled_qty = float(entry_resp.get('executedQty') or normalized['quantity'])
                    tp_resp = active.place_limit_sell(candidate.symbol, quantity=filled_qty, price=normalized['target_price'])
            except Exception as exc:
                if margin_error:
                    raise RuntimeError(f'margin failed ({margin_error}); spot fallback failed ({exc}); margin_attempts={margin_attempts}') from exc
                raise

        position = self.positions.create_position(
            symbol=candidate.symbol,
            side=candidate.side,
            quantity=filled_qty,
            entry_price=avg_fill,
            mark_price=avg_fill,
            stop_price=normalized.get('stop_price'),
            target_price=normalized.get('target_price'),
            meta={
                'candidate_id': candidate.candidate_id,
                'mode': execution_mode,
                'symbol': candidate.symbol,
                'exchange': exchange_name,
                'entry_exchange_order_id': entry_resp.get('orderId'),
                'leverage': used_leverage,
                'margin_attempts': margin_attempts,
            },
        )
        entry_order = self.orders.create_order(
            candidate_id=candidate.candidate_id,
            position_id=position.position_id,
            symbol=candidate.symbol,
            side='buy' if candidate.side == 'long' else 'sell',
            order_type='market',
            quantity=filled_qty,
            requested_price=candidate.entry_price,
            filled_price=avg_fill,
            status=str(entry_resp.get('status', 'filled')).lower(),
            meta={'mode': execution_mode, 'exchange': exchange_name, 'exchange_payload': entry_resp, 'margin_error': margin_error, 'leverage': used_leverage, 'margin_attempts': margin_attempts},
        )
        fill = self.fills.create_fill(order_id=entry_order.order_id, position_id=position.position_id, symbol=candidate.symbol, side='buy', quantity=filled_qty, price=avg_fill)

        tp_local = self.orders.create_order(
            candidate_id=candidate.candidate_id,
            position_id=position.position_id,
            symbol=candidate.symbol,
            side='sell' if candidate.side == 'long' else 'buy',
            order_type='take_profit',
            quantity=filled_qty,
            requested_price=normalized['target_price'],
            filled_price=None,
            status=str(tp_resp.get('status', 'open')).lower(),
            meta={'mode': execution_mode, 'exchange': exchange_name, 'exchange_payload': tp_resp, 'exchange_order_id': tp_resp.get('orderId')},
        )
        position.meta = {
            **(position.meta or {}),
            'tp_local_order_id': tp_local.order_id,
            'tp_exchange_order_id': tp_resp.get('orderId'),
            'exit_strategy': 'take_profit_stop' if normalized.get('stop_price') else 'take_profit_only',
        }
        stop_local = None
        if normalized.get('stop_price') is not None:
            stop_resp = active.place_stop_loss(candidate.symbol, candidate.side, filled_qty, normalized['stop_price'])
            stop_local = self.orders.create_order(
                candidate_id=candidate.candidate_id,
                position_id=position.position_id,
                symbol=candidate.symbol,
                side='sell' if candidate.side == 'long' else 'buy',
                order_type='stop_loss',
                quantity=filled_qty,
                requested_price=normalized['stop_price'],
                filled_price=None,
                status=str(stop_resp.get('status', 'open')).lower(),
                meta={'mode': 'live', 'exchange': exchange_name, 'exchange_payload': stop_resp, 'exchange_order_id': stop_resp.get('orderId')},
            )
            position.meta = {**(position.meta or {}), 'stop_local_order_id': stop_local.order_id, 'stop_exchange_order_id': stop_resp.get('orderId')}
        self.db.commit()
        self.db.refresh(position)
        self.candidates.mark_executed(candidate.candidate_id)
        return {
            'candidate_id': candidate.candidate_id,
            'position_id': position.position_id,
            'entry_order_id': entry_order.order_id,
            'fill_id': fill.fill_id,
            'tp_order_id': tp_local.order_id,
            'mode': execution_mode,
            'exchange': exchange_name,
            'exchange_entry_order_id': entry_resp.get('orderId'),
            'exchange_tp_order_id': tp_resp.get('orderId'),
            'stop_order_id': stop_local.order_id if stop_local else None,
        }

    def execute_open_candidates(self, limit: int = 100, quantity: float = 1.0, mode: str = 'paper') -> dict:
        executed = []
        skipped = []
        requested_mode = (mode or 'paper').lower()
        for candidate in self.candidates.get_open_candidates(limit=limit):
            if candidate.stage == 'momentum':
                skipped.append({'candidate_id': candidate.candidate_id, 'reason': 'momentum_candidate_handled_by_momentum_executor'})
                continue
            if candidate.entry_price is None:
                skipped.append({'candidate_id': candidate.candidate_id, 'reason': 'missing_entry_price'})
                continue
            try:
                mark_price = self._current_price_for_candidate(candidate, requested_mode=requested_mode)
                if not self._price_before_target(candidate, mark_price):
                    skipped.append({
                        'candidate_id': candidate.candidate_id,
                        'reason': 'current_price_past_or_missing_target',
                        'mark_price': mark_price,
                        'target_price': candidate.target_price,
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

    def _momentum_response_base(self, decision: dict, *, action: str | None = None) -> dict:
        return {
            'decision_action': (action or decision.get('decision_action') or 'HOLD').upper(),
            'symbol': decision.get('symbol'),
            'target_symbol': decision.get('target_symbol'),
            'order_ids': [],
            'fill_ids': [],
        }

    def _open_position_for_symbol(self, *symbols: str | None):
        wanted = {str(symbol).upper() for symbol in symbols if symbol}
        if not wanted:
            return None
        for position in self.positions.list_positions(limit=500, status='open'):
            if position.symbol.upper() in wanted:
                return position
        return None

    def _close_momentum_position(self, position, *, reason: str, mode: str) -> dict:
        mark_price = position.mark_price if position.mark_price is not None else position.entry_price
        if mode == 'live':
            active = self.kraken
            if not active.is_configured():
                exchange_name = getattr(active, 'exchange_name', getattr(self.exchange, 'exchange_name', 'kraken'))
                raise RuntimeError(f'{exchange_name} live trading requested but API credentials are missing')
            position_mode = str((position.meta or {}).get('mode') or '').lower()
            if position_mode in {'cross_margin', 'isolated_margin', 'margin'}:
                exit_resp = self._kraken_margin_order_manager().sell_all_margin_base(symbol=position.symbol)
                mark_price = float(exit_resp.get('price') or mark_price or 0.0)
                status = str(exit_resp.get('status', 'filled')).lower()
                meta = {'mode': position_mode or 'cross_margin', 'reason': reason, 'exchange_payload': exit_resp, 'exchange_order_id': exit_resp.get('order_id') or exit_resp.get('orderId')}
            elif hasattr(active, 'place_market_exit'):
                exit_resp = active.place_market_exit(position.symbol, position.side, float(position.quantity))
                mark_price = active.average_fill_price(exit_resp, fallback=mark_price) if hasattr(active, 'average_fill_price') else mark_price
                status = str(exit_resp.get('status', 'filled')).lower()
                meta = {'mode': 'live', 'reason': reason, 'exchange_payload': exit_resp, 'exchange_order_id': exit_resp.get('orderId')}
            elif position.side == 'long' and hasattr(active, 'place_market_sell'):
                exit_resp = active.place_market_sell(position.symbol, float(position.quantity))
                mark_price = active.average_fill_price(exit_resp, fallback=mark_price) if hasattr(active, 'average_fill_price') else mark_price
                status = str(exit_resp.get('status', 'filled')).lower()
                meta = {'mode': 'live', 'reason': reason, 'exchange_payload': exit_resp, 'exchange_order_id': exit_resp.get('orderId')}
            else:
                raise RuntimeError('live momentum sell is not supported by the configured execution adapter')
        else:
            exit_resp = {}
            status = 'filled'
            meta = {'mode': 'paper', 'reason': reason}

        quantity = float(position.quantity)
        fill_price = float(mark_price or 0.0)
        pnl = None
        if position.entry_price is not None:
            if position.side == 'short':
                pnl = (float(position.entry_price) - fill_price) * quantity
            else:
                pnl = (fill_price - float(position.entry_price)) * quantity
        order = self.orders.create_order(
            candidate_id=(position.meta or {}).get('candidate_id'),
            position_id=position.position_id,
            symbol=position.symbol,
            side='buy' if position.side == 'short' else 'sell',
            order_type='market',
            quantity=quantity,
            requested_price=mark_price,
            filled_price=fill_price,
            status=status,
            meta=meta,
        )
        fill = self.fills.create_fill(order_id=order.order_id, position_id=position.position_id, symbol=position.symbol, side=order.side, quantity=quantity, price=fill_price)
        self.positions.close_position(position.position_id, mark_price=fill_price, unrealized_pnl=pnl)
        return {'position_id': position.position_id, 'order_id': order.order_id, 'fill_id': fill.fill_id, 'mode': mode, 'exchange_order_id': exit_resp.get('orderId')}

    def _candidate_for_momentum_decision(self, decision: dict, *, symbol: str | None) -> TradeCandidate | None:
        candidate_id = decision.get('candidate_id')
        if candidate_id:
            candidate = self.db.get(TradeCandidate, candidate_id)
            if candidate is not None:
                return candidate
        if not symbol:
            return None
        stmt = (
            select(TradeCandidate)
            .where(TradeCandidate.status == 'open', TradeCandidate.stage == 'momentum', TradeCandidate.symbol == symbol.upper())
            .order_by(TradeCandidate.score.desc(), TradeCandidate.created_at.asc())
            .limit(1)
        )
        return self.db.scalars(stmt).first()

    def _execute_momentum_buy(self, decision: dict, *, symbol: str | None, quantity: float, requested_mode: str) -> dict:
        base = self._momentum_response_base(decision, action='BUY')
        candidate = self._candidate_for_momentum_decision(decision, symbol=symbol)
        if candidate is None:
            return {**base, 'symbol': symbol, 'target_symbol': decision.get('target_symbol') or symbol, 'status': 'skipped', 'reason': 'missing_open_momentum_candidate'}
        base = {**base, 'symbol': candidate.symbol, 'target_symbol': decision.get('target_symbol') or candidate.symbol}
        if candidate.entry_price is None:
            return {**base, 'status': 'skipped', 'reason': 'missing_entry_price'}
        mark_price = self._current_price_for_candidate(candidate, requested_mode=requested_mode)
        if not self._price_before_target(candidate, mark_price):
            return {**base, 'status': 'skipped', 'reason': 'current_price_past_or_missing_target', 'mark_price': mark_price, 'target_price': candidate.target_price}
        result = self._execute_live_momentum_leveraged_buy(candidate, quantity) if requested_mode == 'live' else self._execute_paper_candidate(candidate, quantity)
        order_ids = [result.get('order_id'), result.get('entry_order_id'), result.get('tp_order_id'), result.get('stop_order_id')]
        fill_ids = [result.get('fill_id')]
        return {**base, 'status': 'executed', 'order_ids': [item for item in order_ids if item], 'fill_ids': [item for item in fill_ids if item], 'reason': 'momentum_candidate_executed', 'result': result}

    def execute_momentum_decision(self, quantity: float = 1.0, mode: str = 'paper') -> dict:
        from app.services.momentum_decision_service import MomentumDecisionService

        requested_mode = (mode or 'paper').lower()
        decision = MomentumDecisionService(self.db).decision()
        action = str(decision.get('decision_action') or 'HOLD').upper()
        decision = {**decision, 'decision_action': action}
        base = self._momentum_response_base(decision, action=action)

        if action not in SUPPORTED_MOMENTUM_EXECUTOR_ACTIONS:
            return {**base, 'status': 'skipped', 'reason': f'unsupported_momentum_decision_action:{action}'}
        if action in {'WAIT', 'HOLD'}:
            return {**base, 'status': 'skipped', 'reason': decision.get('reason') or 'momentum_decision_noop'}
        if str(decision.get('status') or '').lower() != 'ready':
            return {**base, 'status': 'skipped', 'reason': decision.get('reason') or 'momentum_decision_not_ready'}
        if action == 'BUY':
            try:
                return self._execute_momentum_buy(decision, symbol=decision.get('target_symbol') or decision.get('symbol'), quantity=quantity, requested_mode=requested_mode)
            except Exception as exc:
                return {**base, 'status': 'error', 'reason': str(exc)}
        if action == 'SELL':
            try:
                position = self._open_position_for_symbol(decision.get('symbol'), decision.get('target_symbol'))
                if position is None:
                    return {**base, 'status': 'skipped', 'reason': 'no_open_position_for_momentum_sell'}
                result = self._close_momentum_position(position, reason='momentum_sell', mode=requested_mode)
                return {**base, 'symbol': position.symbol, 'target_symbol': decision.get('target_symbol'), 'status': 'executed', 'reason': 'momentum_position_closed', 'order_ids': [result['order_id']], 'fill_ids': [result['fill_id']], 'result': result}
            except Exception as exc:
                return {**base, 'status': 'error', 'reason': str(exc)}
        if action == 'ROTATE':
            close_result = None
            order_ids = []
            fill_ids = []
            try:
                position = self._open_position_for_symbol(decision.get('symbol'))
                if position is None:
                    return {**base, 'status': 'skipped', 'reason': 'no_open_source_position_for_momentum_rotate'}
                close_result = self._close_momentum_position(position, reason='momentum_rotate_exit', mode=requested_mode)
                order_ids.append(close_result['order_id'])
                fill_ids.append(close_result['fill_id'])
                buy_result = self._execute_momentum_buy(decision, symbol=decision.get('target_symbol'), quantity=quantity, requested_mode=requested_mode)
                order_ids.extend(buy_result.get('order_ids') or [])
                fill_ids.extend(buy_result.get('fill_ids') or [])
                status = 'executed' if buy_result.get('status') == 'executed' else 'skipped'
                reason = 'momentum_rotated' if status == 'executed' else f"rotation_exit_completed_entry_{buy_result.get('status', 'unknown')}"
                return {**base, 'status': status, 'reason': reason, 'order_ids': order_ids, 'fill_ids': fill_ids, 'exit_result': close_result, 'entry_result': buy_result}
            except Exception as exc:
                if close_result is not None:
                    return {**base, 'status': 'error', 'reason': f'rotation_exit_completed_entry_error: {exc}', 'order_ids': order_ids, 'fill_ids': fill_ids, 'exit_result': close_result}
                return {**base, 'status': 'error', 'reason': str(exc)}
        return {**base, 'status': 'skipped', 'reason': f'unsupported_momentum_decision_action:{action}'}

    def _take_profit_exchange_order_id(self, position, meta: dict) -> str | None:
        direct = meta.get('tp_exchange_order_id') or meta.get('exchange_tp_order_id')
        if direct:
            return str(direct)
        local_order_id = meta.get('tp_local_order_id') or meta.get('take_profit_local_order_id')
        local_order = self.db.get(Order, local_order_id) if local_order_id else None
        if local_order is None:
            local_order = self.db.scalars(
                select(Order)
                .where(Order.position_id == position.position_id, Order.order_type == 'take_profit')
                .order_by(Order.created_at.desc())
                .limit(1)
            ).first()
        if local_order is None:
            return None
        local_meta = local_order.meta or {}
        local_exchange_id = local_meta.get('exchange_order_id') or local_meta.get('tp_exchange_order_id') or local_meta.get('orderId')
        return str(local_exchange_id) if local_exchange_id else None

    def reconcile_live_positions(self) -> dict:
        if not self._runtime_section('live').get('live_reconcile_enabled'):
            return {'enabled': False, 'checked': 0, 'closed': [], 'updated': []}
        checked = 0
        closed = []
        updated = []
        for position in self.positions.list_positions(limit=500, status='open'):
            meta = position.meta or {}
            supported_live_modes = {'live', 'cross_margin', 'isolated_margin', 'margin', 'spot'}
            if str(meta.get('mode') or '').lower() not in supported_live_modes:
                continue
            checked += 1
            symbol = position.symbol
            try:
                mark = self.kraken.current_price(symbol)
                position.mark_price = mark
                if position.entry_price is not None and position.quantity is not None:
                    position.unrealized_pnl = (mark - float(position.entry_price)) * float(position.quantity)
                self.db.commit()
                self.db.refresh(position)
                updated.append({'position_id': position.position_id, 'mark_price': mark})

                tp_exchange_id = self._take_profit_exchange_order_id(position, meta)
                if not tp_exchange_id:
                    updated.append({'position_id': position.position_id, 'reason': 'missing_take_profit_order'})
                    continue
                tp_status = self.kraken.get_order(symbol, tp_exchange_id)

                if tp_status and str(tp_status.get('status', '')).upper() == 'FILLED':
                    fill_price = float(tp_status.get('price') or position.target_price or mark)
                    self.positions.close_position(position.position_id, mark_price=fill_price, unrealized_pnl=((fill_price - float(position.entry_price)) * float(position.quantity)))
                    closed.append({'position_id': position.position_id, 'reason': 'tp', 'fill_price': fill_price})
            except Exception as exc:
                updated.append({'position_id': position.position_id, 'error': str(exc)})
        return {'enabled': True, 'checked': checked, 'closed': closed, 'updated': updated}

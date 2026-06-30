from sqlalchemy.orm import Session

from app.core.config import settings
from app.services.exchange_adapter import create_execution_adapter
from app.services.fill_service import FillService
from app.services.order_service import OrderService
from app.services.position_service import PositionService
from app.services.risk_service import RiskService
from app.services.momentum_candidate_sync_service import MomentumCandidateSyncService
from app.services.trade_candidate_service import TradeCandidateService


class ExecutorService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.orders = OrderService(db)
        self.fills = FillService(db)
        self.positions = PositionService(db)
        self.candidates = TradeCandidateService(db)
        self.risk = RiskService(db)
        self.exchange = create_execution_adapter()
        self.binance = self.exchange  # backward-compatible test/extension hook

    def _is_short_side(self, side: str | None) -> bool:
        return (side or '').lower() in {'short', 'sell', 'bear'}

    def _current_price_for_candidate(self, candidate, *, requested_mode: str) -> float:
        if requested_mode == 'live':
            return self.binance.current_price(candidate.symbol)
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

    def _execute_live_candidate(self, candidate, quantity: float) -> dict:
        self.risk.validate_live_candidate(symbol=candidate.symbol, side=candidate.side, entry_price=candidate.entry_price, stop_price=candidate.stop_price, target_price=candidate.target_price, quantity=quantity)
        active = self.binance
        exchange_name = getattr(active, 'exchange_name', getattr(self.exchange, 'exchange_name', 'binance'))
        if not active.is_configured():
            raise RuntimeError(f'{exchange_name} live trading requested but API credentials are missing')
        if candidate.side != 'long' and not settings.live_spot_allow_shorts:
            raise RuntimeError('Live short execution is not supported in current spot mode')

        stop_for_exchange = candidate.stop_price if hasattr(active, 'place_market_entry') else None
        normalized = active.normalize_order(candidate.symbol, quantity=quantity, target_price=candidate.target_price, stop_price=stop_for_exchange)
        if hasattr(active, 'place_market_entry'):
            entry_resp = active.place_market_entry(candidate.symbol, candidate.side, normalized['quantity'])
            avg_fill = active.average_fill_price(entry_resp, fallback=candidate.entry_price or normalized['mark_price']) or candidate.entry_price or normalized['mark_price']
        else:
            entry_resp = active.place_market_buy(candidate.symbol, normalized['quantity'])
            avg_fill = active.average_fill_price(entry_resp) or candidate.entry_price or normalized['mark_price']
        filled_qty = float(entry_resp.get('executedQty') or normalized['quantity'])

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
                'mode': 'live',
                'symbol': candidate.symbol,
                'exchange': exchange_name,
                'entry_exchange_order_id': entry_resp.get('orderId'),
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
            meta={'mode': 'live', 'exchange': exchange_name, 'exchange_payload': entry_resp},
        )
        fill = self.fills.create_fill(order_id=entry_order.order_id, position_id=position.position_id, symbol=candidate.symbol, side='buy', quantity=filled_qty, price=avg_fill)

        if hasattr(active, 'place_exit_limit'):
            tp_resp = active.place_exit_limit(candidate.symbol, candidate.side, quantity=filled_qty, price=normalized['target_price'])
        else:
            tp_resp = active.place_limit_sell(candidate.symbol, quantity=filled_qty, price=normalized['target_price'])
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
            meta={'mode': 'live', 'exchange': exchange_name, 'exchange_payload': tp_resp, 'exchange_order_id': tp_resp.get('orderId')},
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
            'mode': 'live',
            'exchange': exchange_name,
            'exchange_entry_order_id': entry_resp.get('orderId'),
            'exchange_tp_order_id': tp_resp.get('orderId'),
            'stop_order_id': stop_local.order_id if stop_local else None,
        }

    def execute_open_candidates(self, limit: int = 100, quantity: float = 1.0, mode: str = 'paper', sync_momentum_first: bool = False) -> dict:
        executed = []
        skipped = []
        sync_result = None
        if sync_momentum_first:
            sync_result = MomentumCandidateSyncService(self.db).sync(limit=limit)
        requested_mode = (mode or 'paper').lower()
        for candidate in self.candidates.get_open_candidates(limit=limit):
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
        result = {'mode': requested_mode, 'executed': executed, 'skipped': skipped}
        if sync_result is not None:
            result['sync'] = sync_result
        return result

    def reconcile_live_positions(self) -> dict:
        if not settings.live_reconcile_enabled:
            return {'enabled': False, 'checked': 0, 'closed': [], 'updated': []}
        checked = 0
        closed = []
        updated = []
        for position in self.positions.list_positions(limit=500, status='open'):
            meta = position.meta or {}
            if meta.get('mode') != 'live':
                continue
            checked += 1
            symbol = position.symbol
            try:
                mark = self.binance.current_price(symbol)
                position.mark_price = mark
                if position.entry_price is not None and position.quantity is not None:
                    position.unrealized_pnl = (mark - float(position.entry_price)) * float(position.quantity)
                self.db.commit()
                self.db.refresh(position)
                updated.append({'position_id': position.position_id, 'mark_price': mark})

                tp_exchange_id = meta.get('tp_exchange_order_id')
                tp_status = self.binance.get_order(symbol, tp_exchange_id) if tp_exchange_id else None

                if tp_status and str(tp_status.get('status', '')).upper() == 'FILLED':
                    fill_price = float(tp_status.get('price') or position.target_price or mark)
                    self.positions.close_position(position.position_id, mark_price=fill_price, unrealized_pnl=((fill_price - float(position.entry_price)) * float(position.quantity)))
                    closed.append({'position_id': position.position_id, 'reason': 'tp', 'fill_price': fill_price})
            except Exception as exc:
                updated.append({'position_id': position.position_id, 'error': str(exc)})
        return {'enabled': True, 'checked': checked, 'closed': closed, 'updated': updated}

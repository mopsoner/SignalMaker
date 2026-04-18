from sqlalchemy.orm import Session

from app.core.config import settings
from app.services.binance_trading_service import BinanceTradingService
from app.services.fill_service import FillService
from app.services.order_service import OrderService
from app.services.position_service import PositionService
from app.services.risk_service import RiskService
from app.services.trade_candidate_service import TradeCandidateService


class ExecutorService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.orders = OrderService(db)
        self.fills = FillService(db)
        self.positions = PositionService(db)
        self.candidates = TradeCandidateService(db)
        self.risk = RiskService(db)
        self.binance = BinanceTradingService()

    def _execute_paper_candidate(self, candidate, quantity: float) -> dict:
        position = self.positions.create_position(symbol=candidate.symbol, side=candidate.side, quantity=quantity, entry_price=candidate.entry_price, mark_price=candidate.entry_price, stop_price=candidate.stop_price, target_price=candidate.target_price, meta={"candidate_id": candidate.candidate_id, "mode": "paper"})
        order = self.orders.create_order(candidate_id=candidate.candidate_id, position_id=position.position_id, symbol=candidate.symbol, side=candidate.side, order_type="market", quantity=quantity, requested_price=candidate.entry_price, filled_price=candidate.entry_price, status="filled", meta={"mode": "paper"})
        fill = self.fills.create_fill(order_id=order.order_id, position_id=position.position_id, symbol=candidate.symbol, side=candidate.side, quantity=quantity, price=candidate.entry_price)
        self.candidates.mark_executed(candidate.candidate_id)
        return {"candidate_id": candidate.candidate_id, "position_id": position.position_id, "order_id": order.order_id, "fill_id": fill.fill_id, "mode": "paper"}

    def _execute_live_candidate(self, candidate, quantity: float) -> dict:
        self.risk.validate_live_candidate(symbol=candidate.symbol, side=candidate.side, entry_price=candidate.entry_price, stop_price=candidate.stop_price, target_price=candidate.target_price, quantity=quantity)
        if not self.binance.is_configured():
            raise RuntimeError('Binance live trading requested but API credentials are missing')
        if candidate.side != 'long' and not settings.live_spot_allow_shorts:
            raise RuntimeError('Live short execution is not supported in current spot mode')

        normalized = self.binance.normalize_order(candidate.symbol, quantity=quantity, target_price=candidate.target_price, stop_price=candidate.stop_price)
        entry_resp = self.binance.place_market_buy(candidate.symbol, normalized['quantity'])
        filled_qty = float(entry_resp.get('executedQty') or normalized['quantity'])
        avg_fill = self.binance.average_fill_price(entry_resp) or candidate.entry_price or normalized['mark_price']

        position = self.positions.create_position(
            symbol=candidate.symbol,
            side='long',
            quantity=filled_qty,
            entry_price=avg_fill,
            mark_price=avg_fill,
            stop_price=normalized.get('stop_price'),
            target_price=normalized.get('target_price'),
            meta={
                'candidate_id': candidate.candidate_id,
                'mode': 'live',
                'symbol': candidate.symbol,
                'entry_exchange_order_id': entry_resp.get('orderId'),
            },
        )
        entry_order = self.orders.create_order(
            candidate_id=candidate.candidate_id,
            position_id=position.position_id,
            symbol=candidate.symbol,
            side='buy',
            order_type='market',
            quantity=filled_qty,
            requested_price=candidate.entry_price,
            filled_price=avg_fill,
            status=str(entry_resp.get('status', 'filled')).lower(),
            meta={'mode': 'live', 'exchange': entry_resp},
        )
        fill = self.fills.create_fill(order_id=entry_order.order_id, position_id=position.position_id, symbol=candidate.symbol, side='buy', quantity=filled_qty, price=avg_fill)

        oco_resp = self.binance.place_oco_sell(
            candidate.symbol,
            quantity=filled_qty,
            take_profit_price=normalized['target_price'],
            stop_price=normalized['stop_price'],
            stop_limit_price=normalized['stop_limit_price'],
        )
        reports = oco_resp.get('orderReports') or []
        tp_report = next((item for item in reports if item.get('type') == 'LIMIT_MAKER'), None)
        sl_report = next((item for item in reports if item.get('type') in ('STOP_LOSS_LIMIT', 'STOP_LOSS')), None)
        tp_local = self.orders.create_order(
            candidate_id=candidate.candidate_id,
            position_id=position.position_id,
            symbol=candidate.symbol,
            side='sell',
            order_type='take_profit',
            quantity=filled_qty,
            requested_price=normalized['target_price'],
            filled_price=None,
            status='open',
            meta={'mode': 'live', 'oco': oco_resp, 'exchange_order_id': tp_report.get('orderId') if tp_report else None},
        )
        sl_local = self.orders.create_order(
            candidate_id=candidate.candidate_id,
            position_id=position.position_id,
            symbol=candidate.symbol,
            side='sell',
            order_type='stop_loss',
            quantity=filled_qty,
            requested_price=normalized['stop_price'],
            filled_price=None,
            status='open',
            meta={'mode': 'live', 'oco': oco_resp, 'exchange_order_id': sl_report.get('orderId') if sl_report else None},
        )
        position.meta = {
            **(position.meta or {}),
            'oco_order_list_id': oco_resp.get('orderListId'),
            'tp_local_order_id': tp_local.order_id,
            'sl_local_order_id': sl_local.order_id,
            'tp_exchange_order_id': tp_report.get('orderId') if tp_report else None,
            'sl_exchange_order_id': sl_report.get('orderId') if sl_report else None,
        }
        self.db.commit()
        self.db.refresh(position)
        self.candidates.mark_executed(candidate.candidate_id)
        return {
            'candidate_id': candidate.candidate_id,
            'position_id': position.position_id,
            'entry_order_id': entry_order.order_id,
            'fill_id': fill.fill_id,
            'tp_order_id': tp_local.order_id,
            'sl_order_id': sl_local.order_id,
            'mode': 'live',
            'exchange_entry_order_id': entry_resp.get('orderId'),
            'exchange_oco_order_list_id': oco_resp.get('orderListId'),
        }

    def execute_open_candidates(self, limit: int = 10, quantity: float = 1.0, mode: str = 'paper') -> dict:
        executed = []
        skipped = []
        requested_mode = (mode or 'paper').lower()
        for candidate in self.candidates.get_open_candidates(limit=limit):
            if candidate.entry_price is None:
                skipped.append({'candidate_id': candidate.candidate_id, 'reason': 'missing_entry_price'})
                continue
            try:
                if requested_mode == 'live':
                    result = self._execute_live_candidate(candidate, quantity)
                else:
                    result = self._execute_paper_candidate(candidate, quantity)
                executed.append(result)
            except Exception as exc:
                skipped.append({'candidate_id': candidate.candidate_id, 'reason': str(exc)})
        return {'mode': requested_mode, 'executed': executed, 'skipped': skipped}

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
                sl_exchange_id = meta.get('sl_exchange_order_id')
                tp_status = self.binance.get_order(symbol, tp_exchange_id) if tp_exchange_id else None
                sl_status = self.binance.get_order(symbol, sl_exchange_id) if sl_exchange_id else None

                filled_report = None
                if tp_status and str(tp_status.get('status', '')).upper() == 'FILLED':
                    filled_report = ('tp', tp_status)
                elif sl_status and str(sl_status.get('status', '')).upper() == 'FILLED':
                    filled_report = ('sl', sl_status)

                if filled_report:
                    reason, report = filled_report
                    fill_price = float(report.get('price') or report.get('stopPrice') or mark)
                    self.positions.close_position(position.position_id, mark_price=fill_price, unrealized_pnl=((fill_price - float(position.entry_price)) * float(position.quantity)))
                    if reason == 'tp' and sl_exchange_id:
                        try:
                            self.binance.cancel_order(symbol, sl_exchange_id)
                        except Exception:
                            pass
                    if reason == 'sl' and tp_exchange_id:
                        try:
                            self.binance.cancel_order(symbol, tp_exchange_id)
                        except Exception:
                            pass
                    closed.append({'position_id': position.position_id, 'reason': reason, 'fill_price': fill_price})
            except Exception as exc:
                updated.append({'position_id': position.position_id, 'error': str(exc)})
        return {'enabled': True, 'checked': checked, 'closed': closed, 'updated': updated}

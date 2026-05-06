from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.order import Order
from app.schemas.gateway import GatewayExecutionEvent, GatewayExecutionReport
from app.services.fill_service import FillService
from app.services.order_service import OrderService
from app.services.position_service import PositionService
from app.services.trade_candidate_service import TradeCandidateService


FINAL_EVENT_REASONS = {
    "take_profit_filled": "take_profit",
    "stop_loss_filled": "stop_loss",
    "position_closed": "external_close",
}


class GatewayExecutionService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.positions = PositionService(db)
        self.orders = OrderService(db)
        self.fills = FillService(db)
        self.candidates = TradeCandidateService(db)

    @staticmethod
    def _status(value: str | None) -> str:
        return (value or "new").lower()

    @staticmethod
    def _exchange_order_id(value: str | int | None) -> str | None:
        return str(value) if value is not None else None

    def _find_order_by_exchange_id(self, candidate_id: str, exchange_order_id: str | int | None) -> Order | None:
        exchange_id = self._exchange_order_id(exchange_order_id)
        if not exchange_id:
            return None
        rows = self.db.execute(select(Order).where(Order.candidate_id == candidate_id)).scalars().all()
        for row in rows:
            meta = row.meta or {}
            if str(meta.get("exchange_order_id")) == exchange_id:
                return row
        return None

    def _open_position_for_candidate(self, candidate_id: str):
        return self.positions.get_open_position_for_candidate(candidate_id)

    def record_execution(self, report: GatewayExecutionReport) -> dict[str, Any]:
        candidate = self.db.get(self.candidates.db.get_bind().mapper.class_ if False else type("Dummy", (), {}), report.candidate_id) if False else None
        # Use direct DB lookup through the mapped model kept inside TradeCandidateService internals.
        from app.models.trade_candidate import TradeCandidate

        candidate = self.db.get(TradeCandidate, report.candidate_id)
        if candidate is None:
            raise ValueError(f"Unknown candidate_id: {report.candidate_id}")

        symbol = (report.execution_symbol or report.signal_symbol or report.entry_order.payload.get("symbol") if report.entry_order.payload else None or candidate.symbol).upper()
        side = report.side.lower()
        entry_price = report.entry_order.avg_price or report.entry_order.price or report.entry_price or candidate.entry_price
        filled_qty = report.entry_order.executed_qty or report.quantity
        target_price = report.target_price if report.target_price is not None else candidate.target_price
        stop_price = report.stop_price if report.stop_price is not None else candidate.stop_price

        position = self.positions.create_position(
            symbol=symbol,
            side=side,
            quantity=float(filled_qty),
            entry_price=entry_price,
            mark_price=entry_price,
            stop_price=stop_price,
            target_price=target_price,
            meta={
                "candidate_id": report.candidate_id,
                "gateway_id": report.gateway_id,
                "signal_symbol": report.signal_symbol or candidate.symbol,
                "execution_symbol": symbol,
                "exchange": report.exchange,
                "mode": report.mode,
                "source": "gateway_execution",
                "payload": report.payload or {},
            },
        )

        entry_order = self.orders.create_order(
            candidate_id=report.candidate_id,
            position_id=position.position_id,
            symbol=symbol,
            side="buy" if side == "long" else "sell",
            order_type="market",
            quantity=float(filled_qty),
            requested_price=report.entry_price,
            filled_price=entry_price,
            status=self._status(report.entry_order.status),
            meta={
                "source": "gateway_execution",
                "gateway_id": report.gateway_id,
                "exchange": report.exchange,
                "exchange_order_id": self._exchange_order_id(report.entry_order.exchange_order_id),
                "payload": report.entry_order.payload or {},
            },
        )
        fill = self.fills.create_fill(
            order_id=entry_order.order_id,
            position_id=position.position_id,
            symbol=symbol,
            side=entry_order.side,
            quantity=float(filled_qty),
            price=float(entry_price),
        )

        tp_order = None
        if report.tp_order is not None:
            tp_order = self.orders.create_order(
                candidate_id=report.candidate_id,
                position_id=position.position_id,
                symbol=symbol,
                side="sell" if side == "long" else "buy",
                order_type="take_profit",
                quantity=float(filled_qty),
                requested_price=report.tp_order.price or target_price,
                filled_price=None,
                status=self._status(report.tp_order.status),
                meta={
                    "source": "gateway_execution",
                    "gateway_id": report.gateway_id,
                    "exchange": report.exchange,
                    "exchange_order_id": self._exchange_order_id(report.tp_order.exchange_order_id),
                    "payload": report.tp_order.payload or {},
                },
            )

        sl_order = None
        if report.sl_order is not None:
            sl_order = self.orders.create_order(
                candidate_id=report.candidate_id,
                position_id=position.position_id,
                symbol=symbol,
                side="sell" if side == "long" else "buy",
                order_type="stop_loss",
                quantity=float(filled_qty),
                requested_price=report.sl_order.price or stop_price,
                filled_price=None,
                status=self._status(report.sl_order.status),
                meta={
                    "source": "gateway_execution",
                    "gateway_id": report.gateway_id,
                    "exchange": report.exchange,
                    "exchange_order_id": self._exchange_order_id(report.sl_order.exchange_order_id),
                    "payload": report.sl_order.payload or {},
                },
            )

        self.candidates.mark_executed(report.candidate_id)
        return {
            "candidate_id": report.candidate_id,
            "position_id": position.position_id,
            "entry_order_id": entry_order.order_id,
            "fill_id": fill.fill_id,
            "tp_order_id": tp_order.order_id if tp_order else None,
            "sl_order_id": sl_order.order_id if sl_order else None,
            "status": "recorded",
        }

    def record_event(self, event: GatewayExecutionEvent) -> dict[str, Any]:
        order = self._find_order_by_exchange_id(event.candidate_id, event.exchange_order_id)
        if order is not None:
            next_status = "filled" if event.event_type.endswith("_filled") else "cancelled" if event.event_type == "order_cancelled" else "error" if event.event_type == "execution_error" else order.status
            meta = {**(order.meta or {}), "last_gateway_event": event.model_dump(mode="json")}
            self.orders.update_order(order.order_id, status=next_status, meta=meta)

        position = self._open_position_for_candidate(event.candidate_id)
        closed_position_id = None
        if position is not None and event.event_type in FINAL_EVENT_REASONS:
            reason = event.reason or FINAL_EVENT_REASONS[event.event_type]
            meta = {**(position.meta or {}), "close_reason": reason, "last_gateway_event": event.model_dump(mode="json")}
            position.meta = meta
            self.db.commit()
            self.positions.close_position(position.position_id, mark_price=position.mark_price)
            closed_position_id = position.position_id

            if event.event_type == "take_profit_filled":
                self._cancel_peer_order(event.candidate_id, order_type="stop_loss", event=event)
            if event.event_type == "stop_loss_filled":
                self._cancel_peer_order(event.candidate_id, order_type="take_profit", event=event)

        return {
            "candidate_id": event.candidate_id,
            "event_type": event.event_type,
            "order_id": order.order_id if order else None,
            "position_id": closed_position_id or (position.position_id if position else None),
            "status": "recorded",
        }

    def _cancel_peer_order(self, candidate_id: str, order_type: str, event: GatewayExecutionEvent) -> None:
        rows = self.db.execute(
            select(Order).where(Order.candidate_id == candidate_id, Order.order_type == order_type, Order.status == "open")
        ).scalars().all()
        for row in rows:
            meta = {**(row.meta or {}), "cancel_reason": "oco_peer_filled", "last_gateway_event": event.model_dump(mode="json")}
            self.orders.update_order(row.order_id, status="cancelled", meta=meta)

    def heartbeat(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {"status": "ok", "received_at": datetime.now(timezone.utc).isoformat(), **payload}

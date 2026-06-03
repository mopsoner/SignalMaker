from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import requests
from sqlalchemy.orm import Session

from app.core.config import settings
from app.services.binance_trading_service import BinanceTradingService
from app.services.fill_service import FillService
from app.services.order_service import OrderService
from app.services.position_service import PositionService


class MomentumExecutorService:
    """Raspberry executor bridge for remote momentum rotation decisions.

    It reads SignalMaker main /api/v1/momentum-engine/decision and applies it
    locally in paper or live mode. The goal is to keep the Raspberry workflow
    transparent: BUY, SELL, ROTATE, HOLD, WAIT.
    """

    def __init__(self, db: Session) -> None:
        self.db = db
        self.binance = BinanceTradingService()
        self.positions = PositionService(db)
        self.orders = OrderService(db)
        self.fills = FillService(db)

    def decision_url(self) -> str:
        base = settings.momentum_executor_api_base.rstrip("/")
        path = settings.momentum_executor_decision_path
        if not path.startswith("/"):
            path = "/" + path
        return base + path

    def read_decision(self) -> dict[str, Any]:
        response = requests.get(self.decision_url(), timeout=20)
        response.raise_for_status()
        payload = response.json()
        payload.setdefault("read_at", datetime.now(timezone.utc).isoformat())
        return payload

    def current_momentum_position(self):
        rows = self.positions.list_positions(limit=100, status="open")
        for row in rows:
            meta = row.meta or {}
            if meta.get("mode") in {"momentum_paper", "momentum_live"} or meta.get("strategy") == "momentum_rotation":
                return row
        return None

    def status(self) -> dict[str, Any]:
        try:
            decision = self.read_decision()
        except Exception as exc:
            decision = {"action": "ERROR", "reason": str(exc)}
        position = self.current_momentum_position()
        return {
            "enabled": settings.momentum_executor_enabled,
            "mode": settings.momentum_executor_mode,
            "api_base": settings.momentum_executor_api_base,
            "decision": decision,
            "local_position": self._position_payload(position),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def run_once(self, *, force: bool = False) -> dict[str, Any]:
        if not settings.momentum_executor_enabled and not force:
            return {"enabled": False, "action": "DISABLED", "reason": "MOMENTUM_EXECUTOR_ENABLED=false"}
        decision = self.read_decision()
        action = str(decision.get("action") or "WAIT").upper()
        mode = settings.momentum_executor_mode.lower()
        local_position = self.current_momentum_position()

        if action == "WAIT":
            return {"action": "WAIT", "decision": decision, "local_position": self._position_payload(local_position)}
        if action == "HOLD":
            return {"action": "HOLD", "decision": decision, "local_position": self._position_payload(local_position)}
        if action == "SELL":
            sold = self._sell_local_position(local_position, mode=mode, reason=decision.get("reason") or "momentum_sell")
            return {"action": "SELL", "decision": decision, "sold": sold}
        if action == "BUY":
            bought = self._buy_symbol(str(decision.get("buy_symbol") or decision.get("symbol")), mode=mode, reason=decision.get("reason") or "momentum_buy")
            return {"action": "BUY", "decision": decision, "bought": bought}
        if action == "ROTATE":
            sold = self._sell_local_position(local_position, mode=mode, reason=decision.get("reason") or "momentum_rotate_sell")
            bought = self._buy_symbol(str(decision.get("buy_symbol") or decision.get("symbol")), mode=mode, reason=decision.get("reason") or "momentum_rotate_buy")
            return {"action": "ROTATE", "decision": decision, "sold": sold, "bought": bought}
        return {"action": "UNKNOWN", "decision": decision, "reason": f"Unsupported action {action}"}

    def _buy_symbol(self, symbol: str, *, mode: str, reason: str) -> dict[str, Any]:
        symbol = symbol.upper()
        if not symbol or symbol == "NONE":
            return {"skipped": True, "reason": "missing_symbol"}
        price = self.binance.current_price(symbol)
        notional = float(settings.momentum_executor_notional)
        quantity = notional / price if price else 0.0
        if mode == "live":
            if not settings.live_trading_enabled:
                raise RuntimeError("LIVE_TRADING_ENABLED=false")
            normalized = self.binance.normalize_order(symbol, quantity=quantity, target_price=None, stop_price=None)
            order_payload = self.binance.place_market_buy(symbol, normalized["quantity"])
            filled_qty = float(order_payload.get("executedQty") or normalized["quantity"])
            avg_price = self.binance.average_fill_price(order_payload) or price
            mode_meta = "momentum_live"
            exchange = order_payload
        else:
            filled_qty = quantity
            avg_price = price
            mode_meta = "momentum_paper"
            exchange = None

        position = self.positions.create_position(
            symbol=symbol,
            side="long",
            quantity=filled_qty,
            entry_price=avg_price,
            mark_price=avg_price,
            stop_price=None,
            target_price=None,
            meta={"mode": mode_meta, "strategy": "momentum_rotation", "reason": reason},
        )
        order = self.orders.create_order(
            candidate_id=None,
            position_id=position.position_id,
            symbol=symbol,
            side="buy",
            order_type="market",
            quantity=filled_qty,
            requested_price=price,
            filled_price=avg_price,
            status="filled",
            meta={"mode": mode_meta, "strategy": "momentum_rotation", "exchange": exchange},
        )
        fill = self.fills.create_fill(order_id=order.order_id, position_id=position.position_id, symbol=symbol, side="buy", quantity=filled_qty, price=avg_price)
        return {"symbol": symbol, "position_id": position.position_id, "order_id": order.order_id, "fill_id": fill.fill_id, "quantity": filled_qty, "price": avg_price, "mode": mode_meta}

    def _sell_local_position(self, position, *, mode: str, reason: str) -> dict[str, Any]:
        if not position:
            return {"skipped": True, "reason": "no_local_momentum_position"}
        symbol = position.symbol
        quantity = float(position.quantity or 0.0)
        price = self.binance.current_price(symbol)
        if mode == "live":
            if not settings.live_trading_enabled:
                raise RuntimeError("LIVE_TRADING_ENABLED=false")
            order_payload = self.binance.place_market_sell(symbol, quantity)
            avg_price = self.binance.average_fill_price(order_payload) or price
            mode_meta = "momentum_live"
            exchange = order_payload
        else:
            avg_price = price
            mode_meta = "momentum_paper"
            exchange = None
        pnl = (avg_price - float(position.entry_price or avg_price)) * quantity
        self.positions.close_position(position.position_id, mark_price=avg_price, unrealized_pnl=pnl)
        order = self.orders.create_order(candidate_id=None, position_id=position.position_id, symbol=symbol, side="sell", order_type="market", quantity=quantity, requested_price=price, filled_price=avg_price, status="filled", meta={"mode": mode_meta, "strategy": "momentum_rotation", "reason": reason, "exchange": exchange})
        fill = self.fills.create_fill(order_id=order.order_id, position_id=position.position_id, symbol=symbol, side="sell", quantity=quantity, price=avg_price)
        return {"symbol": symbol, "position_id": position.position_id, "order_id": order.order_id, "fill_id": fill.fill_id, "quantity": quantity, "price": avg_price, "pnl": pnl, "mode": mode_meta}

    def _position_payload(self, position) -> dict[str, Any] | None:
        if not position:
            return None
        return {
            "position_id": position.position_id,
            "symbol": position.symbol,
            "status": position.status,
            "side": position.side,
            "quantity": position.quantity,
            "entry_price": position.entry_price,
            "mark_price": position.mark_price,
            "unrealized_pnl": position.unrealized_pnl,
            "opened_at": position.opened_at,
            "meta": position.meta,
        }

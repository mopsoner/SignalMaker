import os
import time

from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.binance_symbol_rules import BinanceSymbolRules


class SpotOrderManager:
    def __init__(self, binance: BinanceClient, rules: BinanceSymbolRules) -> None:
        self.binance = binance
        self.rules = rules

    @staticmethod
    def _order_id(payload: dict | None):
        if not payload:
            return None
        return payload.get("orderId") or payload.get("order_id")

    @staticmethod
    def _executed_qty(payload: dict, fallback: str | None = None) -> str:
        try:
            value = float(payload.get("executedQty") or 0)
            if value > 0:
                return str(value)
        except Exception:
            pass
        return str(fallback or "0")

    @staticmethod
    def _avg_price_from_order(payload: dict, fallback: float) -> float:
        avg = BinanceClient.average_fill_price(payload, fallback=None)
        if avg is not None:
            return float(avg)
        try:
            qty = float(payload.get("executedQty") or 0)
            quote_qty = float(payload.get("cummulativeQuoteQty") or payload.get("cumulativeQuoteQty") or 0)
            if qty > 0 and quote_qty > 0:
                return quote_qty / qty
        except Exception:
            pass
        return float(fallback)

    def _entry_confirm_timeout_seconds(self) -> float:
        try:
            return max(1.0, float(os.getenv("SPOT_ENTRY_CONFIRM_TIMEOUT_SECONDS", "8") or "8"))
        except Exception:
            return 8.0

    def _entry_confirm_poll_seconds(self) -> float:
        try:
            return max(0.2, float(os.getenv("SPOT_ENTRY_CONFIRM_POLL_SECONDS", "0.5") or "0.5"))
        except Exception:
            return 0.5

    def confirm_spot_entry_order(self, *, symbol: str, entry_order_id, submitted_payload: dict, fallback_price: float) -> dict:
        symbol = symbol.upper()
        if not entry_order_id:
            raise RuntimeError(f"spot_entry_missing_order_id symbol={symbol} payload={submitted_payload}")
        if self.binance.dry_run:
            payload = {**submitted_payload, "status": "FILLED", "confirmed_dry_run": True}
            return {
                "entry_confirmed": True,
                "entry_confirm_status": "FILLED",
                "entry_confirm_payload": payload,
                "entry_price": self._avg_price_from_order(payload, fallback_price),
                "executed_qty": self._executed_qty(payload, submitted_payload.get("quantity")),
            }
        deadline = time.monotonic() + self._entry_confirm_timeout_seconds()
        last_payload = submitted_payload
        while time.monotonic() <= deadline:
            payload = self.binance.get_order(symbol, entry_order_id)
            last_payload = payload
            status = str(payload.get("status") or "").upper()
            side = str(payload.get("side") or "").upper()
            order_symbol = str(payload.get("symbol") or symbol).upper()
            executed_qty = float(payload.get("executedQty") or 0)
            if order_symbol != symbol:
                raise RuntimeError(f"spot_entry_symbol_mismatch expected={symbol} got={order_symbol} order_id={entry_order_id}")
            if side and side != "BUY":
                raise RuntimeError(f"spot_entry_side_mismatch expected=BUY got={side} symbol={symbol} order_id={entry_order_id}")
            if status == "FILLED" and executed_qty > 0:
                return {
                    "entry_confirmed": True,
                    "entry_confirm_status": status,
                    "entry_confirm_payload": payload,
                    "entry_price": self._avg_price_from_order(payload, fallback_price),
                    "executed_qty": self._executed_qty(payload),
                }
            if status in {"CANCELED", "REJECTED", "EXPIRED"}:
                raise RuntimeError(f"spot_entry_not_filled symbol={symbol} order_id={entry_order_id} status={status} payload={payload}")
            time.sleep(self._entry_confirm_poll_seconds())
        raise RuntimeError(f"spot_entry_confirmation_timeout symbol={symbol} order_id={entry_order_id} last_payload={last_payload}")

    @staticmethod
    def _oco_order_ids(payload: dict | None) -> tuple[str | int | None, str | int | None]:
        payload = payload or {}
        tp_order_id = None
        sl_order_id = None

        def classify(row: dict) -> str | None:
            order_type = str(row.get("type") or row.get("origType") or row.get("aboveType") or row.get("belowType") or "").upper()
            if order_type in {"LIMIT_MAKER", "LIMIT"}:
                return "tp"
            if "STOP" in order_type:
                return "sl"
            if row.get("stopPrice") is not None or row.get("belowStopPrice") is not None:
                return "sl"
            return None

        for row in payload.get("orderReports") or []:
            if not isinstance(row, dict):
                continue
            oid = row.get("orderId")
            kind = classify(row)
            if kind == "tp" and tp_order_id is None:
                tp_order_id = oid
            elif kind == "sl" and sl_order_id is None:
                sl_order_id = oid

        for row in payload.get("orders") or []:
            if not isinstance(row, dict):
                continue
            oid = row.get("orderId")
            kind = classify(row)
            if kind == "tp" and tp_order_id is None:
                tp_order_id = oid
            elif kind == "sl" and sl_order_id is None:
                sl_order_id = oid

        orders = [row for row in (payload.get("orders") or []) if isinstance(row, dict)]
        if (tp_order_id is None or sl_order_id is None) and len(orders) >= 2:
            if tp_order_id is None:
                tp_order_id = orders[0].get("orderId")
            if sl_order_id is None:
                sl_order_id = orders[1].get("orderId")
        return tp_order_id, sl_order_id

    def _submit_oco_sell(self, symbol: str, quantity: str, target_price: str, stop_price: str, stop_limit_price: str) -> dict:
        if self.binance.dry_run:
            now = int(time.time())
            return {"orderListId": f"dry-oco-{now}", "contingencyType": "OCO", "symbol": symbol.upper(), "side": "SELL", "quantity": quantity, "aboveType": "LIMIT_MAKER", "abovePrice": target_price, "belowType": "STOP_LOSS_LIMIT", "belowStopPrice": stop_price, "belowPrice": stop_limit_price, "belowTimeInForce": "GTC", "dry_run": True, "orders": [{"orderId": f"dry-tp-{now}", "type": "LIMIT_MAKER", "price": target_price}, {"orderId": f"dry-sl-{now}", "type": "STOP_LOSS_LIMIT", "stopPrice": stop_price, "price": stop_limit_price}]}
        return self.binance._signed("POST", "/api/v3/orderList/oco", {"symbol": symbol.upper(), "side": "SELL", "quantity": quantity, "aboveType": "LIMIT_MAKER", "abovePrice": target_price, "belowType": "STOP_LOSS_LIMIT", "belowStopPrice": stop_price, "belowPrice": stop_limit_price, "belowTimeInForce": "GTC", "newOrderRespType": "FULL"})

    def create_exit_oco_for_open_long(self, *, symbol: str, quantity: float | str, target_price: float, stop_price: float) -> dict:
        symbol = symbol.upper()
        current_price = self.binance.current_price(symbol)
        if not (float(target_price) > current_price > float(stop_price)):
            raise RuntimeError(f"invalid_oco_price_order symbol={symbol} target={target_price} current={current_price} stop={stop_price}")
        if not self.rules.oco_allowed(symbol):
            raise RuntimeError(f"oco_not_allowed_for_symbol:{symbol}")
        exit_qty = self.rules.normalize_exit_quantity(symbol, quantity)
        tp = self.rules.normalize_exit_price(symbol, target_price)
        stop = self.rules.normalize_exit_price(symbol, stop_price)
        stop_limit = self.rules.normalize_exit_price(symbol, float(stop) * 0.999)
        self.rules.ensure_exit_notional(symbol, exit_qty, tp, label="oco_take_profit")
        self.rules.ensure_exit_notional(symbol, exit_qty, stop_limit, label="oco_stop_loss")
        oco = self._submit_oco_sell(symbol, exit_qty, tp, stop, stop_limit)
        tp_order_id, sl_order_id = self._oco_order_ids(oco)
        return {"symbol": symbol, "quantity": exit_qty, "oco_order_list_id": oco.get("orderListId"), "tp_order_id": tp_order_id, "sl_order_id": sl_order_id, "oco_payload": oco}

    def open_long_with_oco(self, *, symbol: str, quote_amount: float, target_price: float, stop_price: float) -> dict:
        symbol = symbol.upper()
        current_price = self.binance.current_price(symbol)
        quantity = self.rules.quantity_from_quote(symbol, quote_amount, current_price, market=True)
        entry = self.binance.place_market_entry(symbol, "long", quantity)
        entry_order_id = self._order_id(entry)
        confirm = self.confirm_spot_entry_order(symbol=symbol, entry_order_id=entry_order_id, submitted_payload=entry, fallback_price=current_price)
        entry_price = float(confirm["entry_price"])
        executed_qty = confirm["executed_qty"]
        result = {"symbol": symbol, "side": "long", "quantity": executed_qty, "entry_price": entry_price, "entry_order_id": entry_order_id, "entry_confirmed": confirm.get("entry_confirmed"), "entry_confirm_status": confirm.get("entry_confirm_status"), "entry_confirm_payload": confirm.get("entry_confirm_payload") or {}, "entry_payload": entry}
        oco_result = self.create_exit_oco_for_open_long(symbol=symbol, quantity=executed_qty, target_price=target_price, stop_price=stop_price)
        result.update({"quantity": oco_result["quantity"], "oco_order_list_id": oco_result.get("oco_order_list_id"), "tp_order_id": oco_result.get("tp_order_id"), "sl_order_id": oco_result.get("sl_order_id"), "oco_payload": oco_result.get("oco_payload") or {}})
        return result

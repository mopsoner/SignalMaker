import os
import time

from raspberry_executor.kraken_client import KrakenClient
from raspberry_executor.kraken_symbol_rules import KrakenSymbolRules


class SpotOrderManager:
    def __init__(self, kraken: KrakenClient, rules: KrakenSymbolRules) -> None:
        self.kraken = kraken
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
        avg = KrakenClient.average_fill_price(payload, fallback=None)
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
            return max(1.0, float(os.getenv("SPOT_ENTRY_CONFIRM_TIMEOUT_SECONDS", "30") or "30"))
        except Exception:
            return 30.0

    def _entry_confirm_poll_seconds(self) -> float:
        try:
            return max(0.2, float(os.getenv("SPOT_ENTRY_CONFIRM_POLL_SECONDS", "0.5") or "0.5"))
        except Exception:
            return 0.5

    def _available_oco_exit_quantity(self, symbol: str, requested_quantity: float | str) -> dict:
        symbol = symbol.upper()
        requested = float(requested_quantity)
        if requested <= 0:
            raise RuntimeError(f"invalid_oco_requested_quantity symbol={symbol} quantity={requested_quantity}")

        base_asset = self.rules.base_asset(symbol)
        if self.kraken.dry_run:
            return {
                "quantity": self.rules.normalize_exit_quantity(symbol, requested),
                "requested_quantity": requested,
                "free_base_qty": requested,
                "base_asset": base_asset,
                "quantity_source": "dry_run_requested_quantity",
            }

        free_qty = self.kraken.free_balance(base_asset)
        if free_qty <= 0:
            raise RuntimeError(f"no_free_balance_for_oco symbol={symbol} base_asset={base_asset} free_qty={free_qty}")

        usable_qty = min(requested, free_qty)
        return {
            "quantity": self.rules.normalize_exit_quantity(symbol, usable_qty),
            "requested_quantity": requested,
            "free_base_qty": free_qty,
            "base_asset": base_asset,
            "quantity_source": "kraken_free_balance" if free_qty < requested else "requested_quantity_confirmed_available",
        }

    def confirm_spot_entry_order(self, *, symbol: str, entry_order_id, submitted_payload: dict, fallback_price: float) -> dict:
        symbol = symbol.upper()
        if not entry_order_id:
            raise RuntimeError(f"spot_entry_missing_order_id symbol={symbol} payload={submitted_payload}")
        if self.kraken.dry_run:
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
            payload = self.kraken.get_order(symbol, entry_order_id)
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
        if self.kraken.dry_run:
            now = int(time.time())
            return {"orderListId": f"dry-oco-{now}", "contingencyType": "OCO", "symbol": symbol.upper(), "side": "SELL", "quantity": quantity, "aboveType": "LIMIT_MAKER", "abovePrice": target_price, "belowType": "STOP_LOSS_LIMIT", "belowStopPrice": stop_price, "belowPrice": stop_limit_price, "belowTimeInForce": "GTC", "dry_run": True, "orders": [{"orderId": f"dry-tp-{now}", "type": "LIMIT_MAKER", "price": target_price}, {"orderId": f"dry-sl-{now}", "type": "STOP_LOSS_LIMIT", "stopPrice": stop_price, "price": stop_limit_price}]}
        return self.kraken._signed("POST", "/api/v3/orderList/oco", {"symbol": symbol.upper(), "side": "SELL", "quantity": quantity, "aboveType": "LIMIT_MAKER", "abovePrice": target_price, "belowType": "STOP_LOSS_LIMIT", "belowStopPrice": stop_price, "belowPrice": stop_limit_price, "belowTimeInForce": "GTC", "newOrderRespType": "FULL"})

    def create_exit_oco_for_open_long(self, *, symbol: str, quantity: float | str, target_price: float, stop_price: float) -> dict:
        symbol = symbol.upper()
        current_price = self.kraken.current_price(symbol)
        if not (float(target_price) > current_price > float(stop_price)):
            raise RuntimeError(f"invalid_oco_price_order symbol={symbol} target={target_price} current={current_price} stop={stop_price}")
        if not self.rules.oco_allowed(symbol):
            raise RuntimeError(f"oco_not_allowed_for_symbol:{symbol}")
        qty_info = self._available_oco_exit_quantity(symbol, quantity)
        exit_qty = qty_info["quantity"]
        tp = self.rules.normalize_exit_price(symbol, target_price)
        stop = self.rules.normalize_exit_price(symbol, stop_price)
        stop_limit = self.rules.normalize_exit_price(symbol, float(stop) * 0.999)
        self.rules.ensure_exit_notional(symbol, exit_qty, tp, label="oco_take_profit")
        self.rules.ensure_exit_notional(symbol, exit_qty, stop_limit, label="oco_stop_loss")
        oco = self._submit_oco_sell(symbol, exit_qty, tp, stop, stop_limit)
        tp_order_id, sl_order_id = self._oco_order_ids(oco)
        return {"symbol": symbol, "quantity": exit_qty, "oco_order_list_id": oco.get("orderListId"), "tp_order_id": tp_order_id, "sl_order_id": sl_order_id, "oco_payload": oco, "oco_quantity_source": qty_info.get("quantity_source"), "oco_requested_quantity": qty_info.get("requested_quantity"), "oco_free_base_qty": qty_info.get("free_base_qty"), "oco_base_asset": qty_info.get("base_asset")}


    def create_exit_take_profit_for_open_long(self, *, symbol: str, quantity: float | str, target_price: float) -> dict:
        symbol = symbol.upper()
        current_price = self.kraken.current_price(symbol)
        if not (float(target_price) > current_price):
            raise RuntimeError(f"invalid_take_profit_price_order symbol={symbol} target={target_price} current={current_price}")
        qty_info = self._available_oco_exit_quantity(symbol, quantity)
        exit_qty = qty_info["quantity"]
        tp = self.rules.normalize_exit_price(symbol, target_price)
        self.rules.ensure_exit_notional(symbol, exit_qty, tp, label="take_profit_limit")
        order = self.kraken.place_exit_limit(symbol, "long", exit_qty, tp)
        return {"symbol": symbol, "quantity": exit_qty, "tp_order_id": self._order_id(order), "tp_payload": order, "exit_strategy": "take_profit_only", "tp_quantity_source": qty_info.get("quantity_source"), "tp_requested_quantity": qty_info.get("requested_quantity"), "tp_free_base_qty": qty_info.get("free_base_qty"), "tp_base_asset": qty_info.get("base_asset")}

    def open_long_with_take_profit(self, *, symbol: str, quote_amount: float, target_price: float) -> dict:
        symbol = symbol.upper()
        current_price = self.kraken.current_price(symbol)
        quantity = self.rules.quantity_from_quote(symbol, quote_amount, current_price, market=True)
        entry = self.kraken.place_market_entry(symbol, "long", quantity)
        entry_order_id = self._order_id(entry)
        confirm = self.confirm_spot_entry_order(symbol=symbol, entry_order_id=entry_order_id, submitted_payload=entry, fallback_price=current_price)
        entry_price = float(confirm["entry_price"])
        executed_qty = confirm["executed_qty"]
        result = {"symbol": symbol, "side": "long", "quantity": executed_qty, "entry_price": entry_price, "entry_order_id": entry_order_id, "entry_confirmed": confirm.get("entry_confirmed"), "entry_confirm_status": confirm.get("entry_confirm_status"), "entry_confirm_payload": confirm.get("entry_confirm_payload") or {}, "entry_payload": entry, "exit_strategy": "take_profit_only"}
        tp_result = self.create_exit_take_profit_for_open_long(symbol=symbol, quantity=executed_qty, target_price=target_price)
        result.update({"quantity": tp_result["quantity"], "tp_order_id": tp_result.get("tp_order_id"), "sl_order_id": None, "oco_order_list_id": None, "tp_payload": tp_result.get("tp_payload") or {}, "tp_quantity_source": tp_result.get("tp_quantity_source"), "tp_requested_quantity": tp_result.get("tp_requested_quantity"), "tp_free_base_qty": tp_result.get("tp_free_base_qty"), "tp_base_asset": tp_result.get("tp_base_asset")})
        return result

    def open_long_with_oco(self, *, symbol: str, quote_amount: float, target_price: float, stop_price: float) -> dict:
        symbol = symbol.upper()
        current_price = self.kraken.current_price(symbol)
        quantity = self.rules.quantity_from_quote(symbol, quote_amount, current_price, market=True)
        entry = self.kraken.place_market_entry(symbol, "long", quantity)
        entry_order_id = self._order_id(entry)
        confirm = self.confirm_spot_entry_order(symbol=symbol, entry_order_id=entry_order_id, submitted_payload=entry, fallback_price=current_price)
        entry_price = float(confirm["entry_price"])
        executed_qty = confirm["executed_qty"]
        result = {"symbol": symbol, "side": "long", "quantity": executed_qty, "entry_price": entry_price, "entry_order_id": entry_order_id, "entry_confirmed": confirm.get("entry_confirmed"), "entry_confirm_status": confirm.get("entry_confirm_status"), "entry_confirm_payload": confirm.get("entry_confirm_payload") or {}, "entry_payload": entry}
        oco_result = self.create_exit_oco_for_open_long(symbol=symbol, quantity=executed_qty, target_price=target_price, stop_price=stop_price)
        result.update({"quantity": oco_result["quantity"], "oco_order_list_id": oco_result.get("oco_order_list_id"), "tp_order_id": oco_result.get("tp_order_id"), "sl_order_id": oco_result.get("sl_order_id"), "oco_payload": oco_result.get("oco_payload") or {}, "oco_quantity_source": oco_result.get("oco_quantity_source"), "oco_requested_quantity": oco_result.get("oco_requested_quantity"), "oco_free_base_qty": oco_result.get("oco_free_base_qty"), "oco_base_asset": oco_result.get("oco_base_asset")})
        return result

import os
import time

from raspberry_executor.binance_client import BinanceClient
from raspberry_executor.binance_symbol_rules import BinanceSymbolRules
from raspberry_executor.margin_client import MarginClient
from raspberry_executor.margin_settings import margin_multiplier, margin_transfer_spot_balance


def amount_str(value: float) -> str:
    return f"{float(value):.8f}".rstrip("0").rstrip(".")


class MarginOrderManager:
    def __init__(self, binance: BinanceClient, margin: MarginClient, rules: BinanceSymbolRules) -> None:
        self.binance = binance
        self.margin = margin
        self.rules = rules

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
            return max(1.0, float(os.getenv("MARGIN_ENTRY_CONFIRM_TIMEOUT_SECONDS", "8") or "8"))
        except Exception:
            return 8.0

    def _entry_confirm_poll_seconds(self) -> float:
        try:
            return max(0.2, float(os.getenv("MARGIN_ENTRY_CONFIRM_POLL_SECONDS", "0.5") or "0.5"))
        except Exception:
            return 0.5

    def confirm_margin_entry_order(self, *, symbol: str, entry_order_id, submitted_payload: dict, fallback_price: float) -> dict:
        """Confirm a submitted margin entry before OCO creation.

        A POST /sapi/v1/margin/order response is not enough to declare a
        protected position. Query the order back until Binance confirms FILLED,
        then use the confirmed executed quantity and entry price for OCO.
        """
        symbol = symbol.upper()
        if not entry_order_id:
            raise RuntimeError(f"margin_entry_missing_order_id symbol={symbol} payload={submitted_payload}")

        if self.margin.dry_run:
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
            payload = self.margin.get_margin_order(symbol, entry_order_id)
            last_payload = payload
            status = str(payload.get("status") or "").upper()
            side = str(payload.get("side") or "").upper()
            order_symbol = str(payload.get("symbol") or symbol).upper()
            executed_qty = float(payload.get("executedQty") or 0)
            if order_symbol != symbol:
                raise RuntimeError(f"margin_entry_symbol_mismatch expected={symbol} got={order_symbol} order_id={entry_order_id}")
            if side and side != "BUY":
                raise RuntimeError(f"margin_entry_side_mismatch expected=BUY got={side} symbol={symbol} order_id={entry_order_id}")
            if status == "FILLED" and executed_qty > 0:
                return {
                    "entry_confirmed": True,
                    "entry_confirm_status": status,
                    "entry_confirm_payload": payload,
                    "entry_price": self._avg_price_from_order(payload, fallback_price),
                    "executed_qty": self._executed_qty(payload),
                }
            if status in {"CANCELED", "REJECTED", "EXPIRED"}:
                raise RuntimeError(f"margin_entry_not_filled symbol={symbol} order_id={entry_order_id} status={status} payload={payload}")
            time.sleep(self._entry_confirm_poll_seconds())

        raise RuntimeError(f"margin_entry_confirmation_timeout symbol={symbol} order_id={entry_order_id} last_payload={last_payload}")

    def quote_asset(self, symbol: str) -> str:
        return str(self.rules.symbol_info(symbol).get("quoteAsset") or "").upper()

    def _available_margin_quote(self, symbol: str, quote: str) -> float | None:
        if self.margin.dry_run:
            return None
        try:
            return float(self.margin.margin_free_balance(symbol, quote))
        except Exception:
            return None

    def _clamp_own_quote_to_available(self, *, symbol: str, quote: str, requested_quote: float, reserve_pct: float = 0.02) -> tuple[float, dict]:
        requested_quote = max(0.0, float(requested_quote))
        info = {"requested_quote_amount": requested_quote, "available_quote_amount": None, "adjusted_quote_amount": requested_quote, "quote_reserve_pct": reserve_pct, "quote_balance_guard": "not_checked"}
        available = self._available_margin_quote(symbol, quote)
        if available is None:
            return requested_quote, info
        usable = max(0.0, available * max(0.0, 1.0 - reserve_pct))
        adjusted = min(requested_quote, usable)
        info.update({"available_quote_amount": available, "adjusted_quote_amount": adjusted, "quote_balance_guard": "clamped" if adjusted < requested_quote else "ok"})
        if adjusted <= 0:
            raise RuntimeError(f"margin_insufficient_quote_balance symbol={symbol.upper()} quote={quote.upper()} required={requested_quote} available={available} usable={usable}")
        return adjusted, info

    def create_margin_oco_sell(self, *, symbol: str, quantity: float | str, target_price: float, stop_price: float) -> dict:
        symbol = symbol.upper()
        current_price = self.binance.current_price(symbol)
        if not (float(target_price) > current_price > float(stop_price)):
            raise RuntimeError(f"invalid_margin_oco_price_order symbol={symbol} target={target_price} current={current_price} stop={stop_price}")
        exit_qty = self.rules.normalize_exit_quantity(symbol, quantity)
        tp = self.rules.normalize_exit_price(symbol, target_price)
        stop = self.rules.normalize_exit_price(symbol, stop_price)
        stop_limit = self.rules.normalize_exit_price(symbol, float(stop) * 0.999)
        self.rules.ensure_exit_notional(symbol, exit_qty, tp, label="margin_oco_take_profit")
        self.rules.ensure_exit_notional(symbol, exit_qty, stop_limit, label="margin_oco_stop_loss")
        oco = self.margin.margin_oco_sell(symbol, exit_qty, tp, stop, stop_limit)
        tp_order_id, sl_order_id = self._oco_order_ids(oco)
        return {"symbol": symbol, "quantity": exit_qty, "oco_order_list_id": oco.get("orderListId"), "tp_order_id": tp_order_id, "sl_order_id": sl_order_id, "oco_payload": oco}

    def open_long_with_margin_oco(self, *, symbol: str, quote_amount: float, target_price: float, stop_price: float) -> dict:
        symbol = symbol.upper()
        self.margin.ensure_isolated_account(symbol)
        quote = self.quote_asset(symbol)
        multiplier = margin_multiplier()
        requested_own_quote = float(quote_amount)
        balance_guard = {"requested_quote_amount": requested_own_quote, "quote_balance_guard": "not_applicable"}
        if not self.margin.isolated or not margin_transfer_spot_balance():
            own_quote, balance_guard = self._clamp_own_quote_to_available(symbol=symbol, quote=quote, requested_quote=requested_own_quote)
        else:
            own_quote = max(0.0, requested_own_quote)

        wanted_borrow_quote = max(0.0, own_quote * max(0.0, multiplier - 1.0))
        borrow_quote = 0.0
        transfer_payload = None
        borrow_payload = {}
        borrow_error = None
        if self.margin.isolated and margin_transfer_spot_balance() and own_quote > 0:
            transfer_payload = self.margin.transfer_spot_to_margin(symbol, quote, amount_str(own_quote))
        if wanted_borrow_quote > 0:
            try:
                max_borrow = self.margin.max_borrowable(symbol, quote)
                borrow_quote = min(wanted_borrow_quote, max_borrow) if max_borrow > 0 else wanted_borrow_quote
                if borrow_quote > 0:
                    borrow_payload = self.margin.borrow(symbol, quote, amount_str(borrow_quote))
            except Exception as exc:
                borrow_quote = 0.0
                borrow_error = str(exc)
                borrow_payload = {"status": "borrow_failed_continued", "error": borrow_error, "wanted_borrow_quote": wanted_borrow_quote}

        total_quote = own_quote + borrow_quote
        if total_quote <= 0:
            raise RuntimeError(f"margin_long_no_quote_available symbol={symbol} borrow_error={borrow_error} balance_guard={balance_guard}")

        current_price = self.binance.current_price(symbol)
        quantity = self.rules.quantity_from_quote(symbol, total_quote, current_price, market=True)
        entry = self.margin.margin_order(symbol, "BUY", quantity, "MARKET")
        entry_order_id = self._order_id(entry)
        confirm = self.confirm_margin_entry_order(symbol=symbol, entry_order_id=entry_order_id, submitted_payload=entry, fallback_price=current_price)
        entry_price = float(confirm["entry_price"])
        executed_qty = confirm["executed_qty"]

        result = {
            "symbol": symbol,
            "side": "long",
            "mode": "margin",
            "margin_isolated": self.margin.isolated,
            "margin_multiplier": multiplier,
            "quote_asset": quote,
            "own_quote_amount": own_quote,
            "requested_own_quote_amount": requested_own_quote,
            "quote_balance_guard": balance_guard,
            "wanted_borrow_quote_amount": wanted_borrow_quote,
            "borrow_quote_amount": borrow_quote,
            "borrow_error": borrow_error,
            "total_quote_amount": total_quote,
            "quantity": executed_qty,
            "entry_price": entry_price,
            "entry_order_id": entry_order_id,
            "entry_confirmed": confirm.get("entry_confirmed"),
            "entry_confirm_status": confirm.get("entry_confirm_status"),
            "entry_confirm_payload": confirm.get("entry_confirm_payload") or {},
            "transfer_payload": transfer_payload or {},
            "borrow_payload": borrow_payload or {},
            "entry_payload": entry,
        }

        try:
            oco_result = self.create_margin_oco_sell(symbol=symbol, quantity=executed_qty, target_price=target_price, stop_price=stop_price)
            result.update({"quantity": oco_result["quantity"], "oco_order_list_id": oco_result.get("oco_order_list_id"), "tp_order_id": oco_result.get("tp_order_id"), "sl_order_id": oco_result.get("sl_order_id"), "oco_payload": oco_result.get("oco_payload") or {}})
        except Exception as exc:
            result.update({"oco_order_list_id": None, "tp_order_id": None, "sl_order_id": None, "oco_payload": {}, "oco_error": str(exc), "needs_oco_repair": True})
        return result

    def open_short_with_margin_borrow_sell(self, *, symbol: str, quote_amount: float) -> dict:
        symbol = symbol.upper()
        self.margin.ensure_isolated_account(symbol)
        base = self.rules.base_asset(symbol)
        price = self.binance.current_price(symbol)
        qty = self.rules.quantity_from_quote(symbol, float(quote_amount) * max(1.0, margin_multiplier()), price, market=True)
        max_borrow = self.margin.max_borrowable(symbol, base)
        if max_borrow > 0:
            qty = self.rules.normalize_market_quantity(symbol, min(float(qty), max_borrow))
        self.rules.ensure_exit_notional(symbol, qty, price, label="margin_short_sell")
        try:
            borrow = self.margin.borrow(symbol, base, qty)
        except Exception as exc:
            return {"status": "skipped", "reason": "borrow_failed", "error": str(exc), "symbol": symbol, "side": "short", "base_asset": base, "borrow_base_amount": qty, "timestamp": int(time.time())}
        sell = self.margin.margin_order(symbol, "SELL", qty, "MARKET")
        entry_price = BinanceClient.average_fill_price(sell, fallback=price) or price
        sold_qty = self._executed_qty(sell, qty)
        return {"status": "sold", "symbol": symbol, "side": "short", "mode": "isolated_margin" if self.margin.isolated else "cross_margin", "margin_isolated": self.margin.isolated, "base_asset": base, "borrow_base_amount": qty, "quantity": sold_qty, "entry_price": float(entry_price), "margin_multiplier": margin_multiplier(), "borrow_payload": borrow, "entry_order_id": self._order_id(sell), "entry_payload": sell, "timestamp": int(time.time())}

    def open_short_cross_margin(self, *, symbol: str, quote_amount: float) -> dict:
        return self.open_short_with_margin_borrow_sell(symbol=symbol, quote_amount=quote_amount)

    def sell_all_margin_base(self, *, symbol: str) -> dict:
        symbol = symbol.upper()
        self.margin.ensure_isolated_account(symbol)
        base = self.rules.base_asset(symbol)
        free_qty = self.margin.margin_free_balance(symbol, base)
        if free_qty <= 0:
            return {"status": "skipped", "reason": f"no_margin_free_balance:{base}", "symbol": symbol, "base_asset": base}
        price = self.binance.current_price(symbol)
        qty = self.rules.normalize_market_quantity(symbol, free_qty)
        self.rules.ensure_exit_notional(symbol, qty, price, label="margin_sell_on_short")
        order = self.margin.margin_order(symbol, "SELL", qty, "MARKET")
        return {"status": "sold", "symbol": symbol, "base_asset": base, "quantity": qty, "price": price, "order": order, "timestamp": int(time.time())}

import os
import time

from raspberry_executor.kraken_client import KrakenClient
from raspberry_executor.kraken_symbol_rules import KrakenSymbolRules
from raspberry_executor.margin_client import MarginClient
from raspberry_executor.margin_settings import margin_multiplier, margin_transfer_spot_balance
from raspberry_executor.execution_core import place_leveraged_market_entry


def amount_str(value: float) -> str:
    return f"{float(value):.8f}".rstrip("0").rstrip(".")


class MarginOrderManager:
    def __init__(self, kraken: KrakenClient, margin: MarginClient, rules: KrakenSymbolRules) -> None:
        self.kraken = kraken
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
    def _merge_entry_metadata(payload: dict, submitted_payload: dict) -> dict:
        merged = dict(payload or {})
        for key in ("leverage", "requested_leverage", "margin_multiplier", "exchange", "entry_request_payload"):
            if key not in merged and key in (submitted_payload or {}):
                merged[key] = submitted_payload[key]
        return merged

    @staticmethod
    def _executed_qty(payload: dict, fallback: str | None = None) -> str:
        raw = payload.get("executedQty")
        try:
            if float(raw or 0) > 0:
                return str(raw)
        except Exception:
            pass
        return str(fallback or "0")

    @staticmethod
    def _avg_price_from_order(payload: dict, fallback: float) -> float:
        avg = KrakenClient.average_fill_price(payload, fallback=None)
        if avg is not None:
            return float(avg)
        descr = payload.get("descr") if isinstance(payload.get("descr"), dict) else {}
        try:
            price = payload.get("price") or descr.get("price")
            if price and float(price) > 0:
                return float(price)
        except Exception:
            pass
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
            return max(1.0, float(os.getenv("MARGIN_ENTRY_CONFIRM_TIMEOUT_SECONDS", "30") or "30"))
        except Exception:
            return 30

    def _entry_confirm_poll_seconds(self) -> float:
        try:
            return max(1.0, float(os.getenv("MARGIN_ENTRY_CONFIRM_POLL_SECONDS", "5") or "5"))
        except Exception:
            return 5

    def confirm_margin_order(self, *, symbol: str, order_id, submitted_payload: dict, fallback_price: float, expected_side: str) -> dict:
        symbol = symbol.upper()
        expected_side = expected_side.upper()
        if not order_id:
            raise RuntimeError(f"margin_order_missing_order_id symbol={symbol} side={expected_side} payload={submitted_payload}")

        if self.margin.dry_run:
            payload = self._merge_entry_metadata({**submitted_payload, "status": "FILLED", "confirmed_dry_run": True}, submitted_payload)
            return {
                "entry_confirmed": True,
                "entry_confirm_status": "FILLED",
                "entry_confirm_payload": payload,
                "entry_price": self._avg_price_from_order(payload, fallback_price),
                "executed_qty": self._executed_qty(payload, submitted_payload.get("quantity")),
            }
        if str(submitted_payload.get("status") or "").upper() in {"FILLED", "CLOSED"} and float(submitted_payload.get("executedQty") or 0) > 0:
            return {
                "entry_confirmed": True,
                "entry_confirm_status": "FILLED",
                "entry_confirm_payload": self._merge_entry_metadata(submitted_payload, submitted_payload),
                "entry_price": self._avg_price_from_order(submitted_payload, fallback_price),
                "executed_qty": self._executed_qty(submitted_payload, submitted_payload.get("quantity")),
            }

        
        deadline = time.monotonic() + self._entry_confirm_timeout_seconds()
        last_payload = submitted_payload
        while time.monotonic() <= deadline:
            payload = self._merge_entry_metadata(self.margin.get_margin_order(symbol, order_id), submitted_payload)
            last_payload = payload
            status = str(payload.get("status") or "").upper()
            side = str(payload.get("side") or "").upper()
            order_symbol = str(payload.get("symbol") or symbol).upper()
            executed_qty = float(payload.get("executedQty") or 0)
            if order_symbol != symbol:
                raise RuntimeError(f"margin_order_symbol_mismatch expected={symbol} got={order_symbol} order_id={order_id}")
            if side and side != expected_side:
                raise RuntimeError(f"margin_order_side_mismatch expected={expected_side} got={side} symbol={symbol} order_id={order_id}")
            if status in {"FILLED", "CLOSED"} and executed_qty > 0:
                return {
                    "entry_confirmed": True,
                    "entry_confirm_status": status,
                    "entry_confirm_payload": payload,
                    "entry_price": self._avg_price_from_order(payload, fallback_price),
                    "executed_qty": self._executed_qty(payload),
                }
            if status in {"CANCELED", "REJECTED", "EXPIRED"}:
                raise RuntimeError(f"margin_order_not_filled symbol={symbol} order_id={order_id} side={expected_side} status={status} payload={payload}")
            time.sleep(self._entry_confirm_poll_seconds())
        raise RuntimeError(f"margin_order_confirmation_timeout symbol={symbol} order_id={order_id} side={expected_side} last_payload={last_payload}")

    def confirm_margin_entry_order(self, *, symbol: str, entry_order_id, submitted_payload: dict, fallback_price: float) -> dict:
        return self.confirm_margin_order(symbol=symbol, order_id=entry_order_id, submitted_payload=submitted_payload, fallback_price=fallback_price, expected_side="BUY")

    def quote_asset(self, symbol: str) -> str:
        try:
            return str(self.rules.symbol_info(symbol).get("quoteAsset") or "").upper()
        except AttributeError:
            upper = symbol.upper()
            for quote in ("USDC", "USDT", "USD", "EUR", "BTC", "ETH"):
                if upper.endswith(quote):
                    return quote
            return ""

    def _available_margin_quote(self, symbol: str, quote: str) -> float | None:
        if self.margin.dry_run:
            return None
        try:
            return float(self.margin.margin_free_balance(symbol, quote))
        except Exception:
            return None

    def _clamp_quote_to_available(self, *, symbol: str, quote: str, requested_quote: float, available: float | None, balance_source: str, reserve_pct: float = 0.02) -> tuple[float, dict]:
        requested_quote = max(0.0, float(requested_quote))
        info = {"requested_quote_amount": requested_quote, "available_quote_amount": None, "adjusted_quote_amount": requested_quote, "quote_reserve_pct": reserve_pct, "quote_balance_source": balance_source, "quote_balance_guard": "not_checked"}
        if available is None:
            return requested_quote, info
        usable = max(0.0, float(available) * max(0.0, 1.0 - reserve_pct))
        adjusted = min(requested_quote, usable)
        info.update({"available_quote_amount": float(available), "adjusted_quote_amount": adjusted, "quote_balance_guard": "clamped" if adjusted < requested_quote else "ok"})
        if adjusted <= 0:
            raise RuntimeError(f"margin_insufficient_quote_balance symbol={symbol.upper()} quote={quote.upper()} source={balance_source} required={requested_quote} available={available} usable={usable}")
        return adjusted, info

    def _clamp_own_quote_to_available(self, *, symbol: str, quote: str, requested_quote: float, reserve_pct: float = 0.0) -> tuple[float, dict]:
        return self._clamp_quote_to_available(symbol=symbol, quote=quote, requested_quote=requested_quote, available=self._available_margin_quote(symbol, quote), balance_source="margin", reserve_pct=reserve_pct)

    def create_margin_oco_sell(self, *, symbol: str, quantity: float | str, target_price: float, stop_price: float) -> dict:
        symbol = symbol.upper()
        current_price = self.kraken.current_price(symbol)
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



    def place_margin_market_entry(self, *, symbol: str, quote_amount: float, min_notional: float | None = None, leverage: float | str | None = None, clamp_to_available: bool = False) -> dict:
        """Open a leveraged long margin entry with a BUY MARKET order.

        Kraken Spot margin borrows implicitly from the AddOrder `leverage`
        field, so this primitive intentionally does not call borrow/repay.
        """
        symbol = symbol.upper()
        self.margin.ensure_isolated_account(symbol)
        quote = self.quote_asset(symbol)
        effective_leverage = str(leverage if leverage is not None else (self.margin.leverage() if hasattr(self.margin, "leverage") else margin_multiplier()))
        requested_own_quote = float(quote_amount)
        if clamp_to_available:
            own_quote, balance_guard = self._clamp_own_quote_to_available(symbol=symbol, quote=quote, requested_quote=requested_own_quote)
        else:
            own_quote = max(0.0, requested_own_quote)
            available = self._available_margin_quote(symbol, quote)
            balance_guard = {
                "requested_quote_amount": requested_own_quote,
                "available_quote_amount": available,
                "adjusted_quote_amount": own_quote,
                "quote_balance_guard": "diagnostic_only" if available is not None else "not_checked",
                "quote_balance_source": "margin" if available is not None else "caller",
                "clamp_to_available": False,
            }
        if min_notional is not None and own_quote < float(min_notional):
            raise RuntimeError(f"margin_entry_notional_below_minimum symbol={symbol} quote={quote} usable={own_quote} minimum={float(min_notional)} balance_guard={balance_guard}")
        try:
            leverage_float = max(1.0, float(effective_leverage))
        except Exception:
            leverage_float = max(1.0, float(margin_multiplier()))
        leverage_notional = max(0.0, own_quote * max(0.0, leverage_float - 1.0))
        total_quote = own_quote + leverage_notional
        if total_quote <= 0:
            raise RuntimeError(f"margin_long_no_quote_available symbol={symbol} balance_guard={balance_guard}")

        return place_leveraged_market_entry(
            kraken=self.kraken,
            margin=self.margin,
            rules=self.rules,
            symbol=symbol,
            quote_amount=quote_amount,
            leverage=effective_leverage,
            min_notional=min_notional,
            clamp_quote=lambda _symbol, _quote, requested: (own_quote, balance_guard),
            confirm_entry=lambda confirm_symbol, order_id, payload, fallback_price: self.confirm_margin_entry_order(symbol=confirm_symbol, entry_order_id=order_id, submitted_payload=payload, fallback_price=fallback_price),
        )

    open_leveraged_market_entry = place_margin_market_entry

    def create_margin_take_profit_sell(self, *, symbol: str, quantity: float | str, target_price: float) -> dict:
        symbol = symbol.upper()
        current_price = self.kraken.current_price(symbol)
        if not (float(target_price) > current_price):
            raise RuntimeError(f"invalid_margin_take_profit_price_order symbol={symbol} target={target_price} current={current_price}")
        exit_qty = self.rules.normalize_exit_quantity(symbol, quantity)
        tp = self.rules.normalize_exit_price(symbol, target_price)
        self.rules.ensure_exit_notional(symbol, exit_qty, tp, label="margin_take_profit_limit")
        order = self.margin.margin_order(symbol, "SELL", exit_qty, "LIMIT", price=tp, time_in_force="GTC")
        return {"symbol": symbol, "quantity": exit_qty, "tp_order_id": self._order_id(order), "tp_payload": order, "exit_strategy": "take_profit_only"}

    def open_long_with_margin_take_profit(self, *, symbol: str, quote_amount: float, target_price: float, leverage: float | str | None = None) -> dict:
        result = self.place_margin_market_entry(symbol=symbol, quote_amount=quote_amount, leverage=leverage)
        result["exit_strategy"] = "take_profit_only"
        executed_qty = result["quantity"]
        try:
            tp_result = self.create_margin_take_profit_sell(symbol=symbol, quantity=executed_qty, target_price=target_price)
            result.update({"quantity": tp_result["quantity"], "oco_order_list_id": None, "tp_order_id": tp_result.get("tp_order_id"), "sl_order_id": None, "tp_payload": tp_result.get("tp_payload") or {}})
        except Exception as exc:
            result.update({"oco_order_list_id": None, "tp_order_id": None, "sl_order_id": None, "tp_payload": {}, "tp_error": str(exc), "needs_tp_replay": True})
        return result

    def open_long_with_margin_oco(self, *, symbol: str, quote_amount: float, target_price: float, stop_price: float) -> dict:
        result = self.place_margin_market_entry(symbol=symbol, quote_amount=quote_amount)
        executed_qty = result["quantity"]
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
        price = self.kraken.current_price(symbol)
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
        sell_order_id = self._order_id(sell)
        confirm = self.confirm_margin_order(symbol=symbol, order_id=sell_order_id, submitted_payload=sell, fallback_price=price, expected_side="SELL")
        sold_qty = confirm["executed_qty"]
        entry_price = float(confirm["entry_price"])
        return {"status": "sold", "symbol": symbol, "side": "short", "mode": "isolated_margin" if self.margin.isolated else "cross_margin", "margin_isolated": self.margin.isolated, "base_asset": base, "borrow_base_amount": qty, "quantity": sold_qty, "entry_price": entry_price, "margin_multiplier": margin_multiplier(), "borrow_payload": borrow, "entry_order_id": sell_order_id, "entry_confirmed": confirm.get("entry_confirmed"), "entry_confirm_status": confirm.get("entry_confirm_status"), "entry_confirm_payload": confirm.get("entry_confirm_payload") or {}, "entry_payload": sell, "timestamp": int(time.time())}

    def open_short_cross_margin(self, *, symbol: str, quote_amount: float) -> dict:
        return self.open_short_with_margin_borrow_sell(symbol=symbol, quote_amount=quote_amount)

    def sell_all_margin_base(self, *, symbol: str, quantity: float | str | None = None) -> dict:
        symbol = symbol.upper()
        self.margin.ensure_isolated_account(symbol)
        base = self.rules.base_asset(symbol)
        requested_qty = None if quantity is None else float(quantity)
        try:
            free_qty = self.margin.margin_free_balance(symbol, base)
        except Exception:
            free_qty = None
        source_qty = requested_qty if requested_qty is not None else free_qty
        quantity_source = "position_quantity" if requested_qty is not None else "margin_free_balance"
        if source_qty is None or float(source_qty) <= 0:
            reason = "position_quantity_missing" if requested_qty is not None else f"no_margin_free_balance:{base}"
            return {"status": "skipped", "reason": reason, "symbol": symbol, "base_asset": base, "margin_free_balance": free_qty, "quantity_source": quantity_source}
        price = self.kraken.current_price(symbol)
        qty = self.rules.normalize_market_quantity(symbol, source_qty)
        self.rules.ensure_exit_notional(symbol, qty, price, label="margin_sell_on_short")
        order = self.margin.margin_order(symbol, "SELL", qty, "MARKET")
        order_id = self._order_id(order)
        confirm = self.confirm_margin_order(symbol=symbol, order_id=order_id, submitted_payload=order, fallback_price=price, expected_side="SELL")
        return {"status": "sold", "symbol": symbol, "base_asset": base, "quantity": confirm.get("executed_qty"), "requested_quantity": requested_qty, "normalized_quantity": qty, "quantity_source": quantity_source, "margin_free_balance": free_qty, "price": float(confirm.get("entry_price") or price), "order_id": order_id, "order": order, "entry_confirmed": confirm.get("entry_confirmed"), "entry_confirm_status": confirm.get("entry_confirm_status"), "entry_confirm_payload": confirm.get("entry_confirm_payload") or {}, "timestamp": int(time.time())}

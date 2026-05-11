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
        orders = (payload or {}).get("orders") or []
        first = orders[0].get("orderId") if len(orders) > 0 and isinstance(orders[0], dict) else None
        second = orders[1].get("orderId") if len(orders) > 1 and isinstance(orders[1], dict) else None
        return first, second

    @staticmethod
    def _order_id(payload: dict | None):
        if not payload:
            return None
        return payload.get("orderId") or payload.get("order_id")

    @staticmethod
    def _executed_qty(payload: dict, fallback: str) -> str:
        try:
            value = float(payload.get("executedQty") or 0)
            return str(value) if value > 0 else str(fallback)
        except Exception:
            return str(fallback)

    def quote_asset(self, symbol: str) -> str:
        return str(self.rules.symbol_info(symbol).get("quoteAsset") or "").upper()

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
        return {
            "symbol": symbol,
            "quantity": exit_qty,
            "oco_order_list_id": oco.get("orderListId"),
            "tp_order_id": tp_order_id,
            "sl_order_id": sl_order_id,
            "oco_payload": oco,
        }

    def open_long_with_margin_oco(self, *, symbol: str, quote_amount: float, target_price: float, stop_price: float) -> dict:
        symbol = symbol.upper()
        self.margin.ensure_isolated_account(symbol)
        quote = self.quote_asset(symbol)
        multiplier = margin_multiplier()
        own_quote = float(quote_amount)
        borrow_quote = max(0.0, own_quote * max(0.0, multiplier - 1.0))
        transfer_payload = None
        borrow_payload = None

        # Cross margin is now the primary market. Do not transfer spot funds into cross
        # before entry: if the buy fails, this avoids useless SPOT -> MARGIN transfers.
        # Cross should be funded directly in Binance, and the bot can borrow there.
        if self.margin.isolated and margin_transfer_spot_balance() and own_quote > 0:
            transfer_payload = self.margin.transfer_spot_to_margin(symbol, quote, amount_str(own_quote))

        if borrow_quote > 0:
            max_borrow = self.margin.max_borrowable(symbol, quote)
            if max_borrow > 0:
                borrow_quote = min(borrow_quote, max_borrow)
            borrow_payload = self.margin.borrow(symbol, quote, amount_str(borrow_quote))

        # In cross mode, own_quote represents existing funds already available in cross.
        # In isolated mode, own_quote may have been transferred above.
        total_quote = own_quote + borrow_quote
        current_price = self.binance.current_price(symbol)
        quantity = self.rules.quantity_from_quote(symbol, total_quote, current_price, market=True)
        entry = self.margin.margin_order(symbol, "BUY", quantity, "MARKET")
        entry_price = BinanceClient.average_fill_price(entry, fallback=current_price)
        if entry_price is None:
            entry_price = current_price
        executed_qty = self._executed_qty(entry, quantity)
        oco_result = self.create_margin_oco_sell(
            symbol=symbol,
            quantity=executed_qty,
            target_price=target_price,
            stop_price=stop_price,
        )
        return {
            "symbol": symbol,
            "side": "long",
            "mode": "margin",
            "margin_isolated": self.margin.isolated,
            "margin_multiplier": multiplier,
            "quote_asset": quote,
            "own_quote_amount": own_quote,
            "borrow_quote_amount": borrow_quote,
            "total_quote_amount": total_quote,
            "quantity": oco_result["quantity"],
            "entry_price": float(entry_price),
            "entry_order_id": self._order_id(entry),
            "oco_order_list_id": oco_result.get("oco_order_list_id"),
            "tp_order_id": oco_result.get("tp_order_id"),
            "sl_order_id": oco_result.get("sl_order_id"),
            "transfer_payload": transfer_payload or {},
            "borrow_payload": borrow_payload or {},
            "entry_payload": entry,
            "oco_payload": oco_result.get("oco_payload") or {},
        }

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
        borrow = self.margin.borrow(symbol, base, qty)
        sell = self.margin.margin_order(symbol, "SELL", qty, "MARKET")
        entry_price = BinanceClient.average_fill_price(sell, fallback=price) or price
        sold_qty = self._executed_qty(sell, qty)
        return {
            "status": "sold",
            "symbol": symbol,
            "side": "short",
            "mode": "isolated_margin" if self.margin.isolated else "cross_margin",
            "margin_isolated": self.margin.isolated,
            "base_asset": base,
            "borrow_base_amount": qty,
            "quantity": sold_qty,
            "entry_price": float(entry_price),
            "margin_multiplier": margin_multiplier(),
            "borrow_payload": borrow,
            "entry_order_id": self._order_id(sell),
            "entry_payload": sell,
            "timestamp": int(time.time()),
        }

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
        return {
            "status": "sold",
            "symbol": symbol,
            "base_asset": base,
            "quantity": qty,
            "price": price,
            "order": order,
            "timestamp": int(time.time()),
        }

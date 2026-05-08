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
    def _executed_qty(payload: dict, fallback: str) -> str:
        try:
            value = float(payload.get("executedQty") or 0)
            return str(value) if value > 0 else str(fallback)
        except Exception:
            return str(fallback)

    @staticmethod
    def _oco_order_ids(payload: dict | None) -> tuple[str | int | None, str | int | None]:
        orders = (payload or {}).get("orders") or []
        first = orders[0].get("orderId") if len(orders) > 0 and isinstance(orders[0], dict) else None
        second = orders[1].get("orderId") if len(orders) > 1 and isinstance(orders[1], dict) else None
        return first, second

    def _submit_oco_sell(self, symbol: str, quantity: str, target_price: str, stop_price: str, stop_limit_price: str) -> dict:
        if self.binance.dry_run:
            now = int(time.time())
            return {
                "orderListId": f"dry-oco-{now}",
                "contingencyType": "OCO",
                "symbol": symbol.upper(),
                "side": "SELL",
                "quantity": quantity,
                "aboveType": "LIMIT_MAKER",
                "abovePrice": target_price,
                "belowType": "STOP_LOSS_LIMIT",
                "belowStopPrice": stop_price,
                "belowPrice": stop_limit_price,
                "belowTimeInForce": "GTC",
                "dry_run": True,
                "orders": [
                    {"orderId": f"dry-tp-{now}", "type": "LIMIT_MAKER", "price": target_price},
                    {"orderId": f"dry-sl-{now}", "type": "STOP_LOSS_LIMIT", "stopPrice": stop_price, "price": stop_limit_price},
                ],
            }
        return self.binance._signed("POST", "/api/v3/orderList/oco", {
            "symbol": symbol.upper(),
            "side": "SELL",
            "quantity": quantity,
            "aboveType": "LIMIT_MAKER",
            "abovePrice": target_price,
            "belowType": "STOP_LOSS_LIMIT",
            "belowStopPrice": stop_price,
            "belowPrice": stop_limit_price,
            "belowTimeInForce": "GTC",
            "newOrderRespType": "FULL",
        })

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
        return {
            "symbol": symbol,
            "quantity": exit_qty,
            "oco_order_list_id": oco.get("orderListId"),
            "tp_order_id": tp_order_id,
            "sl_order_id": sl_order_id,
            "oco_payload": oco,
        }

    def open_long_with_oco(self, *, symbol: str, quote_amount: float, target_price: float, stop_price: float) -> dict:
        symbol = symbol.upper()
        current_price = self.binance.current_price(symbol)
        quantity = self.rules.quantity_from_quote(symbol, quote_amount, current_price, market=True)
        entry = self.binance.place_market_entry(symbol, "long", quantity)
        entry_price = BinanceClient.average_fill_price(entry, fallback=current_price)
        if entry_price is None:
            raise RuntimeError("unable_to_determine_entry_fill_price")
        executed_qty = self._executed_qty(entry, quantity)
        oco_result = self.create_exit_oco_for_open_long(
            symbol=symbol,
            quantity=executed_qty,
            target_price=target_price,
            stop_price=stop_price,
        )
        return {
            "symbol": symbol,
            "side": "long",
            "quantity": oco_result["quantity"],
            "entry_price": float(entry_price),
            "entry_order_id": self._order_id(entry),
            "oco_order_list_id": oco_result.get("oco_order_list_id"),
            "tp_order_id": oco_result.get("tp_order_id"),
            "sl_order_id": oco_result.get("sl_order_id"),
            "entry_payload": entry,
            "oco_payload": oco_result.get("oco_payload") or {},
        }

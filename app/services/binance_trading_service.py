import hashlib
import hmac
import math
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any
from urllib.parse import urlencode

import requests

from app.core.config import settings


class BinanceTradingService:
    def __init__(self) -> None:
        self.api_key = settings.binance_api_key
        self.secret_key = settings.binance_secret_key
        self.session = requests.Session()

    def rest_base(self) -> str:
        if settings.binance_use_testnet:
            return settings.binance_testnet_rest_base.rstrip('/')
        return settings.binance_rest_base.rstrip('/')

    def is_configured(self) -> bool:
        return bool(self.api_key and self.secret_key)

    def _signed_request(self, method: str, path: str, params: dict[str, Any] | None = None) -> Any:
        if not self.is_configured():
            raise RuntimeError('Binance API keys are not configured')
        payload = dict(params or {})
        payload['timestamp'] = int(time.time() * 1000)
        payload.setdefault('recvWindow', 5000)
        query = urlencode(payload, doseq=True)
        signature = hmac.new(self.secret_key.encode(), query.encode(), hashlib.sha256).hexdigest()
        query = f'{query}&signature={signature}'
        headers = {'X-MBX-APIKEY': self.api_key}
        url = f"{self.rest_base()}{path}?{query}"
        response = self.session.request(method.upper(), url, headers=headers, timeout=20)
        response.raise_for_status()
        return response.json()

    def _public_request(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.rest_base()}{path}"
        response = self.session.get(url, params=params or {}, timeout=20)
        response.raise_for_status()
        return response.json()

    def account(self) -> dict[str, Any]:
        return self._signed_request('GET', '/api/v3/account')

    def symbol_info(self, symbol: str) -> dict[str, Any]:
        data = self._public_request('/api/v3/exchangeInfo', {'symbol': symbol.upper()})
        symbols = data.get('symbols', [])
        if not symbols:
            raise RuntimeError(f'No exchange info for {symbol}')
        return symbols[0]

    def current_price(self, symbol: str) -> float:
        data = self._public_request('/api/v3/ticker/price', {'symbol': symbol.upper()})
        return float(data['price'])

    def get_order(self, symbol: str, order_id: int) -> dict[str, Any]:
        return self._signed_request('GET', '/api/v3/order', {'symbol': symbol.upper(), 'orderId': int(order_id)})

    def get_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params = {'symbol': symbol.upper()} if symbol else None
        return self._signed_request('GET', '/api/v3/openOrders', params)

    def cancel_order(self, symbol: str, order_id: int) -> dict[str, Any]:
        return self._signed_request('DELETE', '/api/v3/order', {'symbol': symbol.upper(), 'orderId': int(order_id)})

    def _filters(self, symbol: str) -> dict[str, dict[str, Any]]:
        info = self.symbol_info(symbol)
        return {item['filterType']: item for item in info.get('filters', [])}

    @staticmethod
    def _quantize(value: float, step: str) -> float:
        q = Decimal(str(step))
        return float(Decimal(str(value)).quantize(q, rounding=ROUND_DOWN))

    def normalize_order(self, symbol: str, quantity: float, target_price: float | None, stop_price: float | None) -> dict[str, Any]:
        filters = self._filters(symbol)
        lot_filter = filters.get('LOT_SIZE') or {}
        price_filter = filters.get('PRICE_FILTER') or {}
        min_notional_filter = filters.get('MIN_NOTIONAL') or filters.get('NOTIONAL') or {}

        step_size = lot_filter.get('stepSize', '0.00000001')
        min_qty = float(lot_filter.get('minQty', 0.0) or 0.0)
        tick_size = price_filter.get('tickSize', '0.00000001')
        min_notional = float(min_notional_filter.get('minNotional', 0.0) or 0.0)

        mark = self.current_price(symbol)
        normalized_qty = self._quantize(quantity, step_size)
        if normalized_qty < min_qty:
            raise RuntimeError(f'Quantity {normalized_qty} below minQty {min_qty} for {symbol}')
        if normalized_qty * mark < min_notional:
            raise RuntimeError(f'Notional {normalized_qty * mark:.4f} below minNotional {min_notional} for {symbol}')

        out = {
            'quantity': normalized_qty,
            'mark_price': mark,
        }
        if target_price is not None:
            out['target_price'] = self._quantize(target_price, tick_size)
        if stop_price is not None:
            out['stop_price'] = self._quantize(stop_price, tick_size)
            out['stop_limit_price'] = self._quantize(stop_price * 0.999, tick_size)
        return out

    def place_market_buy(self, symbol: str, quantity: float) -> dict[str, Any]:
        return self._signed_request('POST', '/api/v3/order', {
            'symbol': symbol.upper(),
            'side': 'BUY',
            'type': 'MARKET',
            'quantity': quantity,
            'newOrderRespType': 'FULL',
        })

    def place_oco_sell(self, symbol: str, quantity: float, take_profit_price: float, stop_price: float, stop_limit_price: float | None = None) -> dict[str, Any]:
        stop_limit_price = stop_limit_price or stop_price
        return self._signed_request('POST', '/api/v3/orderList/oco', {
            'symbol': symbol.upper(),
            'side': 'SELL',
            'quantity': quantity,
            'price': take_profit_price,
            'stopPrice': stop_price,
            'stopLimitPrice': stop_limit_price,
            'stopLimitTimeInForce': 'GTC',
        })

    @staticmethod
    def average_fill_price(order_payload: dict[str, Any]) -> float | None:
        fills = order_payload.get('fills') or []
        if fills:
            qty = sum(float(item.get('qty', 0.0)) for item in fills)
            quote = sum(float(item.get('qty', 0.0)) * float(item.get('price', 0.0)) for item in fills)
            if qty > 0:
                return quote / qty
        executed_qty = float(order_payload.get('executedQty', 0.0) or 0.0)
        cummulative_quote_qty = float(order_payload.get('cummulativeQuoteQty', 0.0) or 0.0)
        if executed_qty > 0 and cummulative_quote_qty > 0:
            return cummulative_quote_qty / executed_qty
        return None

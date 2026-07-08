from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from raspberry_executor.kraken_client import KrakenClient
from raspberry_executor.margin_settings import margin_multiplier


def _order_id(payload: dict | None):
    if not payload:
        return None
    return payload.get("orderId") or payload.get("order_id")


def _executed_qty(payload: dict, fallback: str | None = None) -> str:
    raw = payload.get("executedQty")
    try:
        if float(raw or 0) > 0:
            return str(raw)
    except Exception:
        pass
    return str(fallback or "0")


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
    try:
        price = payload.get("price")
        if price and float(price) > 0:
            return float(price)
    except Exception:
        pass
    return float(fallback)


def quote_asset_for_symbol(rules, symbol: str) -> str:
    try:
        return str(rules.symbol_info(symbol).get("quoteAsset") or "").upper()
    except AttributeError:
        upper = symbol.upper()
        for quote in ("USDC", "USDT", "USD", "EUR", "BTC", "ETH"):
            if upper.endswith(quote):
                return quote
        return ""


def place_leveraged_market_entry(
    *,
    kraken,
    margin,
    rules,
    symbol: str,
    quote_amount: float,
    leverage: float | str | None = None,
    min_notional: float | None = None,
    clamp_quote: Callable[[str, str, float], tuple[float, dict]] | None = None,
    confirm_entry: Callable[[str, Any, dict, float], dict] | None = None,
) -> dict:
    """Open a Kraken spot-margin BUY MARKET entry without explicit borrow."""
    symbol = symbol.upper()
    margin.ensure_isolated_account(symbol)
    quote = quote_asset_for_symbol(rules, symbol)
    effective_leverage = str(leverage if leverage is not None else (margin.leverage() if hasattr(margin, "leverage") else margin_multiplier()))
    requested_own_quote = float(quote_amount)
    if clamp_quote is None:
        own_quote = max(0.0, requested_own_quote)
        balance_guard = {"requested_quote_amount": requested_own_quote, "adjusted_quote_amount": own_quote, "quote_balance_guard": "not_checked", "quote_balance_source": "caller"}
    else:
        own_quote, balance_guard = clamp_quote(symbol, quote, requested_own_quote)
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
    current_price = kraken.current_price(symbol)
    quantity = rules.quantity_from_quote(symbol, total_quote, current_price, market=True)
    entry = margin.margin_order(symbol, "BUY", quantity, "MARKET", leverage=effective_leverage)
    entry_order_id = _order_id(entry)
    if confirm_entry is None:
        confirm = {"entry_price": _avg_price_from_order(entry, current_price), "executed_qty": _executed_qty(entry, quantity), "entry_confirmed": str(entry.get("status") or "").upper() == "FILLED", "entry_confirm_status": entry.get("status"), "entry_confirm_payload": entry}
    else:
        confirm = confirm_entry(symbol, entry_order_id, entry, current_price)
    entry_price = float(confirm["entry_price"])
    executed_qty = confirm["executed_qty"]
    implicit_leverage_payload = {"status": "implicit_margin", "exchange": entry.get("exchange"), "symbol": symbol, "quote_asset": quote, "leverage": effective_leverage, "leverage_notional": leverage_notional}
    return {"symbol": symbol, "side": "long", "mode": "cross_margin", "margin_isolated": False, "margin_multiplier": leverage_float, "leverage": effective_leverage, "quote_asset": quote, "own_quote_amount": own_quote, "requested_own_quote_amount": requested_own_quote, "quote_balance_guard": balance_guard, "leverage_notional": leverage_notional, "implicit_margin": True, "implicit_leverage_payload": implicit_leverage_payload, "total_quote_amount": total_quote, "quantity": executed_qty, "entry_price": entry_price, "entry_order_id": entry_order_id, "entry_confirmed": confirm.get("entry_confirmed"), "entry_confirm_status": confirm.get("entry_confirm_status"), "entry_confirm_payload": confirm.get("entry_confirm_payload") or {}, "transfer_payload": {}, "borrow_payload": implicit_leverage_payload, "borrow_quote_amount": leverage_notional, "borrow_error": None, "entry_payload": entry}


def place_spot_market_entry(*, kraken, rules, symbol: str, quote_amount: float) -> dict:
    symbol = symbol.upper()
    price = kraken.current_price(symbol)
    qty = rules.quantity_from_quote(symbol, float(quote_amount), price, market=True)
    order = kraken.place_market_entry(symbol, "long", qty)
    fill_price = kraken.average_fill_price(order, fallback=price) or price
    return {"mode": "spot", "symbol": symbol, "side": "long", "quantity": qty, "entry_price": float(fill_price), "entry_order_id": _order_id(order), "leverage": None, "entry_payload": order, "total_quote_amount": float(quote_amount)}


def open_market_entry_margin_first(*, kraken, rules, symbol: str, quote_amount: float, margin_factory: Callable[[float | str], Any], leverages: Iterable[float | str] = (5, 3), spot_quote_amount: float | None = None) -> dict:
    attempts: list[dict] = []
    for leverage in leverages:
        try:
            entry = place_leveraged_market_entry(kraken=kraken, margin=margin_factory(leverage), rules=rules, symbol=symbol, quote_amount=quote_amount, leverage=leverage)
            entry["margin_attempts"] = attempts
            return entry
        except Exception as exc:
            attempts.append({"leverage": leverage, "error": str(exc), "error_type": type(exc).__name__})
    entry = place_spot_market_entry(kraken=kraken, rules=rules, symbol=symbol, quote_amount=float(spot_quote_amount if spot_quote_amount is not None else quote_amount))
    entry["margin_attempts"] = attempts
    return entry

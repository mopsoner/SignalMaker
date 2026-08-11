from decimal import Decimal, ROUND_DOWN
from typing import Any


def decimal_value(value: float | str | Decimal) -> Decimal:
    return Decimal(str(value))


def format_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    text = format(normalized, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def floor_to_step(value: float | str | Decimal, step: float | str | Decimal) -> Decimal:
    value_d = decimal_value(value)
    step_d = decimal_value(step)
    if step_d <= 0:
        return value_d
    return (value_d / step_d).to_integral_value(rounding=ROUND_DOWN) * step_d


def filters_by_type(symbol_info: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {item.get("filterType"): item for item in symbol_info.get("filters", [])}


def normalize_quantity(symbol: str, symbol_info: dict[str, Any], quantity: float | str | Decimal, *, market: bool = False) -> str:
    filters = filters_by_type(symbol_info)
    lot = filters.get("MARKET_LOT_SIZE") if market else None
    if not lot or decimal_value(lot.get("stepSize", "0")) <= 0:
        lot = filters.get("LOT_SIZE", {})
    step = lot.get("stepSize", "0")
    min_qty = decimal_value(lot.get("minQty", "0"))
    max_qty = decimal_value(lot.get("maxQty", "0"))
    qty = floor_to_step(quantity, step)
    if qty <= 0:
        raise RuntimeError(f"quantity_too_small_after_step symbol={symbol} quantity={quantity} step={step}")
    if min_qty > 0 and qty < min_qty:
        raise RuntimeError(f"quantity_below_min_qty symbol={symbol} qty={qty} minQty={min_qty}")
    if max_qty > 0 and qty > max_qty:
        qty = floor_to_step(max_qty, step)
    return format_decimal(qty)


def normalize_price(symbol: str, symbol_info: dict[str, Any], price: float | str | Decimal) -> str:
    filters = filters_by_type(symbol_info)
    price_filter = filters.get("PRICE_FILTER", {})
    tick = price_filter.get("tickSize", "0")
    min_price = decimal_value(price_filter.get("minPrice", "0"))
    max_price = decimal_value(price_filter.get("maxPrice", "0"))
    price_d = floor_to_step(price, tick)
    if price_d <= 0:
        raise RuntimeError(f"price_too_small_after_tick symbol={symbol} price={price} tickSize={tick}")
    if min_price > 0 and price_d < min_price:
        raise RuntimeError(f"price_below_min_price symbol={symbol} price={price_d} minPrice={min_price}")
    if max_price > 0 and price_d > max_price:
        price_d = floor_to_step(max_price, tick)
    return format_decimal(price_d)


def min_notional(symbol_info: dict[str, Any]) -> Decimal:
    filters = filters_by_type(symbol_info)
    if filters.get("NOTIONAL"):
        return decimal_value(filters["NOTIONAL"].get("minNotional", "0"))
    if filters.get("MIN_NOTIONAL"):
        return decimal_value(filters["MIN_NOTIONAL"].get("minNotional", "0"))
    return Decimal("0")


def ensure_min_notional(symbol: str, symbol_info: dict[str, Any], quantity: str, price: float | str, *, label: str) -> None:
    required = min_notional(symbol_info)
    if required <= 0:
        return
    notional = decimal_value(quantity) * decimal_value(price)
    if notional < required:
        raise RuntimeError(f"notional_below_min symbol={symbol} label={label} notional={notional} minNotional={required}")

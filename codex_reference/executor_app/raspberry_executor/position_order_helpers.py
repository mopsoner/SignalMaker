from __future__ import annotations


def float_value(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def order_type(order: dict) -> str:
    descr = order.get("descr") if isinstance(order.get("descr"), dict) else {}
    return str(order.get("type") or order.get("origType") or descr.get("ordertype") or "").upper().replace("-", "_")


def order_side(order: dict) -> str:
    descr = order.get("descr") if isinstance(order.get("descr"), dict) else {}
    return str(order.get("side") or order.get("type") or descr.get("type") or "").upper()


def order_qty(order: dict) -> float:
    return float_value(order.get("origQty") or order.get("quantity") or order.get("vol") or order.get("volume") or order.get("executedQty"))


def order_price(order: dict) -> float:
    descr = order.get("descr") if isinstance(order.get("descr"), dict) else {}
    return float_value(order.get("price") or descr.get("price"))


def is_tp_order(order: dict) -> bool:
    return order_side(order) == "SELL" and order_type(order) in {"LIMIT", "LIMIT_MAKER"}

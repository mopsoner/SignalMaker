"""The same strictly positive indicators used by the To The MOOn badge."""
from math import isfinite

PREALERT_FIELDS = ("momentum_delta_15m", "momentum_delta_1h", "momentum_acceleration_1h")


def is_momentum_prealert(asset: dict) -> bool:
    for field in PREALERT_FIELDS:
        value = asset.get(field)
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            return False
        try:
            number = float(value)
        except (ValueError, OverflowError):
            return False
        if not isfinite(number) or number <= 0:
            return False
    return True

from raspberry_executor.config import load_settings
from raspberry_executor.signalmaker_client import SignalMakerClient


_CORRELATION_KEYS = ("candidate_id", "remote_candidate_id", "signal_fingerprint")


def _has_take_profit_level(candidate: dict) -> bool:
    return candidate.get("target_price") is not None


def _normalized(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _float_or_none(value) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def _same_entry_price(left, right) -> bool:
    left_float = _float_or_none(left)
    right_float = _float_or_none(right)
    if left_float is None or right_float is None:
        return False
    return abs(left_float - right_float) <= 1e-9


def _candidate_matches_position(candidate: dict, position: dict) -> bool:
    for key in _CORRELATION_KEYS:
        position_value = _normalized(position.get(key))
        if position_value is None:
            continue
        for candidate_key in _CORRELATION_KEYS:
            if _normalized(candidate.get(candidate_key)) == position_value:
                return True
    if position.get("entry_price") is not None and candidate.get("entry_price") is not None:
        return _same_entry_price(position.get("entry_price"), candidate.get("entry_price"))
    return False


def position_has_candidate_identity(position: dict) -> bool:
    return any(_normalized(position.get(key)) is not None for key in _CORRELATION_KEYS)


def _recent_candidates_for_symbol(symbol: str) -> list[dict]:
    settings = load_settings()
    client = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    if hasattr(client, "get_candidates_for_repair"):
        candidates = client.get_candidates_for_repair(symbol=symbol, limit=100)
    else:
        candidates = client.get_recent_candidates(symbol=symbol, limit=100, use_cursor=False)
    return [c for c in candidates if str(c.get("symbol") or "").upper() == symbol.upper() and _has_take_profit_level(c)]


def _levels_from_candidate(candidate: dict, source: str) -> dict:
    return {
        "target_price": candidate.get("target_price"),
        "stop_price": candidate.get("stop_price"),
        "source_candidate_id": candidate.get("candidate_id"),
        "source_remote_candidate_id": candidate.get("remote_candidate_id"),
        "source_signal_fingerprint": candidate.get("signal_fingerprint"),
        "source": source,
    }


def latest_levels_for_symbol(symbol: str) -> dict | None:
    same_symbol = _recent_candidates_for_symbol(symbol)
    if not same_symbol:
        return None
    same_symbol.sort(key=lambda c: str(c.get("created_at") or c.get("updated_at") or ""), reverse=True)
    return _levels_from_candidate(same_symbol[0], "signalmaker_recent_candidate")


def levels_for_position(position: dict, symbol: str) -> dict | None:
    same_symbol = _recent_candidates_for_symbol(symbol)
    for candidate in same_symbol:
        if _candidate_matches_position(candidate, position):
            return _levels_from_candidate(candidate, "signalmaker_matched_candidate")
    if position_has_candidate_identity(position):
        return None
    if not same_symbol:
        return None
    same_symbol.sort(key=lambda c: str(c.get("created_at") or c.get("updated_at") or ""), reverse=True)
    return _levels_from_candidate(same_symbol[0], "signalmaker_recent_candidate")

from raspberry_executor.config import load_settings
from raspberry_executor.signalmaker_client import SignalMakerClient


def _has_levels(candidate: dict) -> bool:
    return candidate.get("target_price") is not None and candidate.get("stop_price") is not None


def latest_levels_for_symbol(symbol: str) -> dict | None:
    settings = load_settings()
    client = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    candidates = client.get_recent_candidates(symbol=symbol, limit=100)
    same_symbol = [c for c in candidates if str(c.get("symbol") or "").upper() == symbol.upper() and _has_levels(c)]
    if not same_symbol:
        return None
    same_symbol.sort(key=lambda c: str(c.get("created_at") or c.get("updated_at") or ""), reverse=True)
    candidate = same_symbol[0]
    return {
        "target_price": candidate.get("target_price"),
        "stop_price": candidate.get("stop_price"),
        "source_candidate_id": candidate.get("candidate_id"),
        "source": "signalmaker_recent_candidate",
    }

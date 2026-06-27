from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_cache(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_cache(path: str, cache: dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(cache, indent=2, sort_keys=True))


def get_cached_contract(path: str, provider_symbol: str) -> dict[str, Any] | None:
    row = load_cache(path).get(provider_symbol.upper())
    return row if isinstance(row, dict) else None


def upsert_cached_contract(path: str, provider_symbol: str, payload: dict[str, Any]) -> dict[str, Any]:
    cache = load_cache(path)
    key = provider_symbol.upper()
    row = {**payload, "provider_symbol": key, "resolved_at": payload.get("resolved_at") or _now()}
    cache[key] = row
    save_cache(path, cache)
    return row


def mark_error(path: str, provider_symbol: str, error: str) -> dict[str, Any]:
    cache = load_cache(path)
    key = provider_symbol.upper()
    row = dict(cache.get(key) or {"provider_symbol": key})
    row.update({"error": str(error), "error_at": _now()})
    cache[key] = row
    save_cache(path, cache)
    return row

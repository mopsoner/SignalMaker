import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class StateStore:
    def __init__(self, path: str = "state.json") -> None:
        self.path = Path(path)
        self.data = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"executed_candidates": [], "open_positions": {}, "closed_positions": [], "events": []}
        try:
            data = json.loads(self.path.read_text())
            data.setdefault("executed_candidates", [])
            data.setdefault("open_positions", {})
            data.setdefault("closed_positions", [])
            data.setdefault("events", [])
            return data
        except Exception:
            return {"executed_candidates": [], "open_positions": {}, "closed_positions": [], "events": []}

    def save(self) -> None:
        self.path.write_text(json.dumps(self.data, indent=2, sort_keys=True))

    def now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def already_executed(self, candidate_id: str) -> bool:
        return candidate_id in set(self.data.get("executed_candidates", []))

    def mark_executed(self, candidate_id: str) -> None:
        executed = list(self.data.get("executed_candidates", []))
        if candidate_id not in executed:
            executed.append(candidate_id)
        self.data["executed_candidates"] = executed[-500:]
        self.save()

    def add_open_position(self, candidate_id: str, payload: dict[str, Any]) -> None:
        positions = dict(self.data.get("open_positions", {}))
        payload = {**payload, "status": "open", "opened_at": payload.get("opened_at") or self.now()}
        positions[candidate_id] = payload
        self.data["open_positions"] = positions
        self.add_event(candidate_id, "position_opened", payload, save=False)
        self.save()

    def close_position(self, candidate_id: str, reason: str, payload: dict[str, Any] | None = None) -> None:
        positions = dict(self.data.get("open_positions", {}))
        row = positions.pop(candidate_id, None)
        if row is None:
            return
        closed = list(self.data.get("closed_positions", []))
        row = {**row, "status": "closed", "close_reason": reason, "closed_at": self.now(), "close_payload": payload or {}}
        closed.append(row)
        self.data["open_positions"] = positions
        self.data["closed_positions"] = closed[-500:]
        self.add_event(candidate_id, reason, payload or {}, save=False)
        self.save()

    def remove_open_position(self, candidate_id: str) -> None:
        positions = dict(self.data.get("open_positions", {}))
        positions.pop(candidate_id, None)
        self.data["open_positions"] = positions
        self.save()

    def open_positions(self) -> dict[str, Any]:
        return dict(self.data.get("open_positions", {}))

    def closed_positions(self) -> list[dict[str, Any]]:
        return list(self.data.get("closed_positions", []))

    def add_event(self, candidate_id: str, event_type: str, payload: dict[str, Any] | None = None, *, save: bool = True) -> None:
        events = list(self.data.get("events", []))
        events.append({"candidate_id": candidate_id, "event_type": event_type, "timestamp": self.now(), "payload": payload or {}})
        self.data["events"] = events[-1000:]
        if save:
            self.save()

    def events(self) -> list[dict[str, Any]]:
        return list(self.data.get("events", []))

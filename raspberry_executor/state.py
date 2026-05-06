import json
from pathlib import Path
from typing import Any


class StateStore:
    def __init__(self, path: str = "state.json") -> None:
        self.path = Path(path)
        self.data = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"executed_candidates": [], "open_positions": {}}
        try:
            return json.loads(self.path.read_text())
        except Exception:
            return {"executed_candidates": [], "open_positions": {}}

    def save(self) -> None:
        self.path.write_text(json.dumps(self.data, indent=2, sort_keys=True))

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
        positions[candidate_id] = payload
        self.data["open_positions"] = positions
        self.save()

    def remove_open_position(self, candidate_id: str) -> None:
        positions = dict(self.data.get("open_positions", {}))
        positions.pop(candidate_id, None)
        self.data["open_positions"] = positions
        self.save()

    def open_positions(self) -> dict[str, Any]:
        return dict(self.data.get("open_positions", {}))

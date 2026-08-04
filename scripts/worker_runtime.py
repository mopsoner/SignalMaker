import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def heartbeat(worker_id: str, **details) -> None:
    path = ROOT / ".runtime" / f"{worker_id}.heartbeat.json"
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps({"at": datetime.now(timezone.utc).isoformat(), **details}))

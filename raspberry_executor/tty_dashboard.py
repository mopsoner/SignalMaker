import json
import os
import time
from urllib.request import urlopen

URL_BASE = os.getenv("LOCAL_DASHBOARD_URL", "http://127.0.0.1:8090")
REFRESH_SECONDS = float(os.getenv("TTY_DASHBOARD_REFRESH_SECONDS", "5") or "5")


def _fetch(path: str) -> dict:
    try:
        with urlopen(URL_BASE.rstrip("/") + path, timeout=3) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        return {"error": str(exc)}


def _clear() -> None:
    print("\033[2J\033[H", end="")


def _line(char: str = "-") -> None:
    print(char * 80)


def _value(rows: list[dict], label: str) -> str:
    for row in rows:
        if row.get("label") == label:
            return str(row.get("value") or "")
    return ""


def render() -> None:
    status = _fetch("/api/ui/status")
    candidates = _fetch("/api/ui/candidates")
    positions = _fetch("/api/ui/positions")

    _clear()
    print("SignalMaker Raspberry TTY Dashboard")
    _line("=")

    if status.get("error"):
        print("Web API not ready:", status["error"])
        print("Waiting for local dashboard on", URL_BASE)
        return

    status_rows = status.get("rows") or []
    print("Dry run:", _value(status_rows, "Dry run"), " | Order quote:", _value(status_rows, "Order quote"), " | Quote assets:", _value(status_rows, "Quote assets"))
    print("Candidate cursor:", _value(status_rows, "Candidate cursor"))
    print("Last local reset:", _value(status_rows, "Last local reset"))
    _line()

    sync = positions.get("sync") or {}
    print("POSITIONS | open:", len(positions.get("open_rows") or []), "closed:", len(positions.get("closed_rows") or []), "sync:", sync)
    for row in (positions.get("open_rows") or [])[:8]:
        print(f"- {row.get('symbol')} {row.get('side')} qty={row.get('qty')} entry={row.get('entry')} stop={row.get('stop')} target={row.get('target')} TP={row.get('tp_status')} SL={row.get('sl_status')} reason={row.get('reason')}")
    _line()

    summary = candidates.get("summary") or {}
    print("CANDIDATES | total:", summary.get("total"), "received:", summary.get("received"), "executed:", summary.get("executed"), "consumed:", summary.get("consumed"))
    for row in (candidates.get("rows") or [])[:14]:
        print(f"- {row.get('symbol')} {row.get('side')} local={row.get('local_state')} exec={row.get('execution_state')} entry={row.get('entry')} stop={row.get('stop')} target={row.get('target')}")
    _line()
    print("Auto refresh:", REFRESH_SECONDS, "seconds | Ctrl+C to exit")


def main() -> None:
    while True:
        render()
        time.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    main()

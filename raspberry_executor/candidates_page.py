from html import escape

from raspberry_executor.config import load_settings
from raspberry_executor.signalmaker_client import SignalMakerClient
from raspberry_executor.state import StateStore


def c(value):
    return "" if value is None else escape(str(value))


def candidates_page(limit: int = 100) -> str:
    settings = load_settings()
    client = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
    state = StateStore()
    executed = set(state.executed_candidates())
    try:
        open_candidates = client.get_open_candidates(limit=limit)
        recent_candidates = client.get_recent_candidates(limit=limit)
        error = None
    except Exception as exc:
        open_candidates = []
        recent_candidates = []
        error = str(exc)

    body = "<h1>SignalMaker Trade Candidates</h1>"
    body += "<div class='box'>"
    body += f"<p><b>Endpoint:</b> {c(settings.signalmaker_base_url)}/api/v1/trade-candidates</p>"
    body += f"<p><b>Open received:</b> {len(open_candidates)}</p>"
    body += f"<p><b>Recent received:</b> {len(recent_candidates)}</p>"
    if error:
        body += f"<p class='pill bad'>API error: {c(error)}</p>"
    body += "</div>"

    body += "<div class='box'><h2>Open candidates used by executor</h2>"
    body += candidates_table(open_candidates, executed)
    body += "</div>"

    body += "<div class='box'><h2>Recent candidates fallback</h2>"
    body += candidates_table(recent_candidates, executed)
    body += "</div>"
    return body


def candidates_table(rows: list[dict], executed: set[str]) -> str:
    if not rows:
        return "<p class='muted'>No candidates returned.</p>"
    cols = ["Local", "Candidate", "Symbol", "Side", "Status", "Entry", "Stop", "Target", "Created", "Updated"]
    html = "<table><tr>" + "".join(f"<th>{c(col)}</th>" for col in cols) + "</tr>"
    for row in rows:
        cid = str(row.get("candidate_id") or "")
        local = "executed" if cid in executed else "new"
        values = [
            local,
            cid,
            row.get("symbol"),
            row.get("side"),
            row.get("status"),
            row.get("entry_price"),
            row.get("stop_price"),
            row.get("target_price"),
            row.get("created_at"),
            row.get("updated_at"),
        ]
        html += "<tr>" + "".join(f"<td>{c(value)}</td>" for value in values) + "</tr>"
    return html + "</table>"

from html import escape

from raspberry_executor.candidate_view_store import candidate_status_summary, local_candidate_rows
from raspberry_executor.config import load_settings
from raspberry_executor.signalmaker_client import SignalMakerClient


def c(value):
    return "" if value is None else escape(str(value))


def _refresh_remote_candidates(limit: int) -> str | None:
    try:
        settings = load_settings()
        client = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
        client.get_open_candidates(limit=limit)
        client.get_recent_candidates(limit=limit)
        return None
    except Exception as exc:
        return str(exc)


def candidates_page(limit: int = 100) -> str:
    error = _refresh_remote_candidates(limit)
    rows = local_candidate_rows(limit=limit, include_executed=True)
    summary = candidate_status_summary(limit=max(limit, 500))

    body = "<h1>SignalMaker Trade Candidates</h1>"
    body += "<div class='box'>"
    body += f"<p><b>Local total:</b> {summary['total']} | <b>received:</b> {summary['received']} | <b>executed:</b> {summary['executed']} | <b>other:</b> {summary['other']}</p>"
    body += "<p class='muted'>Web and TUI use the same local SQLite view. Unique signal = symbol + side + entry + target + stop.</p>"
    if error:
        body += f"<p class='pill bad'>Remote refresh error: {c(error)}</p>"
    body += "</div>"

    body += "<div class='box'><h2>Local candidates</h2>"
    body += candidates_table(rows)
    body += "</div>"
    return body


def candidates_table(rows: list[dict]) -> str:
    if not rows:
        return "<p class='muted'>No local candidates.</p>"
    cols = ["Local state", "Candidate", "Remote", "Symbol", "Side", "Entry", "Stop", "Target", "First seen", "Last seen", "Fingerprint"]
    html = "<table><tr>" + "".join(f"<th>{c(col)}</th>" for col in cols) + "</tr>"
    for row in rows:
        local = row.get("local_status")
        local_class = "ok" if local == "executed" else "warn" if local == "received" else ""
        values = [
            f"<span class='pill {local_class}'>{c(local)}</span>",
            c(row.get("candidate_id")),
            c(row.get("remote_candidate_id")),
            c(row.get("symbol")),
            c(row.get("side")),
            c(row.get("entry_price")),
            c(row.get("stop_price")),
            c(row.get("target_price")),
            c(row.get("first_seen_at")),
            c(row.get("last_seen_at")),
            c(row.get("signal_fingerprint")),
        ]
        html += "<tr>" + "".join(f"<td>{value}</td>" for value in values) + "</tr>"
    return html + "</table>"

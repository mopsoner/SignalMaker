from html import escape

from raspberry_executor.ui_contract import candidates_view


def c(value):
    return "" if value is None else escape(str(value))


def candidates_page(limit: int = 100) -> str:
    view = candidates_view(limit=limit)
    summary = view["summary"]
    body = f"<h1>{c(view['title'])}</h1>"
    body += "<div class='box'>"
    body += f"<p><b>Local total:</b> {summary['total']} | <b>received:</b> {summary['received']} | <b>executed:</b> {summary['executed']} | <b>other:</b> {summary['other']}</p>"
    body += f"<p class='muted'>{c(view['help'])}</p>"
    body += "<p class='muted'>This page displays local SQLite candidates only. It does not fetch remote candidates.</p>"
    body += "</div>"
    body += "<div class='box'><h2>Local candidates</h2>"
    body += candidates_table(view)
    body += "</div>"
    return body


def candidates_table(view: dict) -> str:
    rows = view.get("rows") or []
    if not rows:
        return f"<p class='muted'>{c(view.get('empty_message') or 'No local candidates.')}</p>"
    labels = view["labels"]
    keys = view["keys"]
    html = "<table><tr>" + "".join(f"<th>{c(label)}</th>" for label in labels) + "</tr>"
    for row in rows:
        html += "<tr>"
        for key in keys:
            value = row.get(key)
            if key == "local_state":
                klass = "ok" if value == "executed" else "warn" if value == "received" else ""
                html += f"<td><span class='pill {klass}'>{c(value)}</span></td>"
            else:
                html += f"<td>{c(value)}</td>"
        html += "</tr>"
    return html + "</table>"

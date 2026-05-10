from html import escape
from http.server import ThreadingHTTPServer
from urllib.parse import parse_qs

from raspberry_executor.env_store import SECRET_KEYS, read_env, write_env
from raspberry_executor.position_sync_v2 import sync_open_positions
from raspberry_executor.reset_positions_db import reset_positions_db
from raspberry_executor.state import StateStore
from raspberry_executor.web_dashboard import Handler as DashboardHandler, page


def cell(value):
    return "" if value is None else escape(str(value))


def order_status(payload):
    if not isinstance(payload, dict):
        return ""
    if payload.get("sync_error"):
        return "sync_error"
    return str(payload.get("status") or "")


def synced_positions_table(rows):
    if not rows:
        return "<p class='muted'>No positions.</p>"
    cols = ["Status", "Candidate", "Symbol", "Side", "Qty", "Entry", "Stop", "Target", "TP", "TP status", "SL", "SL status", "Reason"]
    html = "<table><tr>" + "".join(f"<th>{cell(col)}</th>" for col in cols) + "</tr>"
    for candidate_id, row in rows:
        values = [row.get("status"), candidate_id, row.get("execution_symbol") or row.get("signal_symbol"), row.get("side"), row.get("quantity"), row.get("entry_price"), row.get("stop_price"), row.get("target_price"), row.get("tp_order_id"), order_status(row.get("binance_tp_status")), row.get("sl_order_id"), order_status(row.get("binance_sl_status")), row.get("close_reason")]
        html += "<tr>" + "".join(f"<td>{cell(value)}</td>" for value in values) + "</tr>"
    return html + "</table>"


def positions_page_v2():
    try:
        sync = sync_open_positions()
        sync_html = "<p class='muted'>Binance sync: checked={checked}, closed={closed}, missing_oco={missing_oco}, repaired_oco={repaired_oco}</p>".format(**sync)
    except Exception as exc:
        sync_html = f"<p class='pill bad'>Binance sync unavailable: {cell(exc)}</p>"
    state = StateStore()
    open_rows = list(state.open_positions().items())
    closed_rows = [(item.get("candidate_id", ""), item) for item in reversed(state.closed_positions()[-50:])]
    return "<h1>Binance Synced Positions</h1><div class='box'><h2>Open positions</h2>" + sync_html + synced_positions_table(open_rows) + "</div><div class='box'><h2>Closed positions</h2>" + synced_positions_table(closed_rows) + "</div>"


def admin_page_v2():
    vals = read_env()
    body = "<h1>Admin</h1><div class='box'><h2>Settings</h2><form method='post' action='/admin'>"
    for key in vals:
        field_type = "password" if key in SECRET_KEYS else "text"
        body += f"<label>{escape(key)}</label><input type='{field_type}' name='{escape(key)}' value='{escape(vals.get(key, ''))}'>"
    body += "<button>Save settings</button></form><p class='pill warn'>Restart after changing trading settings.</p></div>"
    body += """
    <div class='box'>
      <h2>Danger zone</h2>
      <p class='muted'>Reset local tracking only: positions, executed candidates, events and pending queue. Binance assets and orders are not modified.</p>
      <form method='post' action='/admin/reset-positions' onsubmit="return confirm('Reset local position tracking tables ?');">
        <button class='danger' type='submit'>Reset positions tracking</button>
      </form>
    </div>
    """
    return body


class Handler(DashboardHandler):
    def do_GET(self):
        if self.path.startswith("/positions"):
            data = page(positions_page_v2())
        elif self.path.startswith("/admin"):
            data = page(admin_page_v2())
        else:
            return super().do_GET()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            return

    def do_POST(self):
        if self.path.startswith("/admin/reset-positions"):
            reset_positions_db()
            self.send_response(303)
            self.send_header("Location", "/positions")
            self.end_headers()
            return
        if self.path.startswith("/admin"):
            length = int(self.headers.get("Content-Length", "0"))
            posted = {k: v[-1] for k, v in parse_qs(self.rfile.read(length).decode()).items()}
            current = read_env()
            for key, value in posted.items():
                if key in current and not (key in SECRET_KEYS and value == "********"):
                    current[key] = value.strip()
            write_env(current)
            self.send_response(303)
            self.send_header("Location", "/admin")
            self.end_headers()
            return
        return super().do_POST()


def run_web(host="0.0.0.0", port=8090):
    ThreadingHTTPServer((host, port), Handler).serve_forever()

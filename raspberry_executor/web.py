from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

import requests

from raspberry_executor.env_store import SECRET_KEYS, public_env, read_env, write_env
from raspberry_executor.logging_setup import tail_logs
from raspberry_executor.state import StateStore


def _page(title: str, body: str) -> bytes:
    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="20">
  <title>{escape(title)}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 20px; background: #111; color: #eee; }}
    a {{ color: #8ab4ff; }}
    nav a {{ margin-right: 16px; }}
    input, select {{ width: 100%; padding: 10px; margin: 6px 0 14px; box-sizing: border-box; background: #222; color: #eee; border: 1px solid #444; }}
    button {{ padding: 10px 16px; background: #2d6cdf; color: white; border: 0; border-radius: 6px; }}
    label {{ font-weight: bold; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
    th, td {{ border-bottom: 1px solid #333; padding: 8px; text-align: left; font-size: 14px; }}
    th {{ color: #aaa; }}
    .box {{ background: #1b1b1b; padding: 14px; border-radius: 8px; margin: 12px 0; }}
    .ok {{ color: #72e37b; }}
    .warn {{ color: #ffd166; }}
    .bad {{ color: #ff6b6b; }}
    .muted {{ color: #aaa; }}
    pre {{ background: #000; padding: 12px; overflow: auto; white-space: pre-wrap; border-radius: 8px; }}
  </style>
</head>
<body>
  <nav><a href="/">Status</a><a href="/positions">Positions</a><a href="/admin">Admin</a><a href="/logs">Logs</a></nav>
  <h1>{escape(title)}</h1>
  {body}
</body>
</html>"""
    return html.encode()


def _cell(value) -> str:
    if value is None:
        return ""
    return escape(str(value))


def _fetch_signalmaker_positions() -> tuple[list[dict], str | None]:
    values = read_env()
    base_url = values.get("SIGNALMAKER_BASE_URL", "").rstrip("/")
    if not base_url or "your-signalmaker" in base_url:
        return [], "SignalMaker URL not configured"
    try:
        response = requests.get(f"{base_url}/api/v1/positions", params={"limit": 50}, timeout=8)
        response.raise_for_status()
        return response.json(), None
    except Exception as exc:
        return [], str(exc)


def _positions_page() -> str:
    state = StateStore()
    local_positions = state.open_positions()
    remote_positions, error = _fetch_signalmaker_positions()

    body = "<div class='box'><h2>Raspberry local positions</h2>"
    if not local_positions:
        body += "<p class='muted'>No local open positions tracked by Raspberry.</p>"
    else:
        body += "<table><tr><th>Candidate</th><th>Signal</th><th>Execution</th><th>Side</th><th>Qty</th><th>Entry order</th><th>TP order</th><th>SL order</th></tr>"
        for candidate_id, row in local_positions.items():
            body += "<tr>"
            body += f"<td>{_cell(candidate_id)}</td>"
            body += f"<td>{_cell(row.get('signal_symbol'))}</td>"
            body += f"<td>{_cell(row.get('execution_symbol'))}</td>"
            body += f"<td>{_cell(row.get('side'))}</td>"
            body += f"<td>{_cell(row.get('quantity'))}</td>"
            body += f"<td>{_cell(row.get('entry_order_id'))}</td>"
            body += f"<td>{_cell(row.get('tp_order_id'))}</td>"
            body += f"<td>{_cell(row.get('sl_order_id'))}</td>"
            body += "</tr>"
        body += "</table>"
    body += "</div>"

    body += "<div class='box'><h2>SignalMaker positions</h2>"
    if error:
        body += f"<p class='warn'>Could not load SignalMaker positions: {_cell(error)}</p>"
    elif not remote_positions:
        body += "<p class='muted'>No positions returned by SignalMaker.</p>"
    else:
        body += "<table><tr><th>Status</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Entry</th><th>Mark</th><th>Stop</th><th>Target</th><th>PnL</th><th>Opened</th></tr>"
        for row in remote_positions:
            status = str(row.get("status", ""))
            klass = "ok" if status == "open" else "muted"
            body += "<tr>"
            body += f"<td class='{klass}'>{_cell(status)}</td>"
            body += f"<td>{_cell(row.get('symbol'))}</td>"
            body += f"<td>{_cell(row.get('side'))}</td>"
            body += f"<td>{_cell(row.get('quantity'))}</td>"
            body += f"<td>{_cell(row.get('entry_price'))}</td>"
            body += f"<td>{_cell(row.get('mark_price'))}</td>"
            body += f"<td>{_cell(row.get('stop_price'))}</td>"
            body += f"<td>{_cell(row.get('target_price'))}</td>"
            body += f"<td>{_cell(row.get('unrealized_pnl'))}</td>"
            body += f"<td>{_cell(row.get('opened_at'))}</td>"
            body += "</tr>"
        body += "</table>"
    body += "</div>"
    return body


class AdminHandler(BaseHTTPRequestHandler):
    def _send(self, title: str, body: str, code: int = 200) -> None:
        data = _page(title, body)
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _redirect(self, path: str) -> None:
        self.send_response(303)
        self.send_header("Location", path)
        self.end_headers()

    def do_GET(self) -> None:
        if self.path.startswith("/positions"):
            self._send("Positions", _positions_page())
            return
        if self.path.startswith("/logs"):
            logs = "\n".join(escape(line) for line in tail_logs(400))
            self._send("Logs", f"<div class='box'><pre>{logs}</pre></div>")
            return
        if self.path.startswith("/admin"):
            values = read_env()
            body = "<form method='post' action='/admin'>"
            fields = [
                ("SIGNALMAKER_BASE_URL", "SignalMaker URL", "text"),
                ("GATEWAY_ID", "Gateway ID", "text"),
                ("POLL_SECONDS", "Poll seconds", "number"),
                ("DRY_RUN", "Dry run true/false", "text"),
                ("EXECUTION_QUOTE_ASSET", "Execution quote asset", "text"),
                ("ALLOWED_SYMBOLS", "Allowed symbols optional", "text"),
                ("ORDER_QUOTE_AMOUNT", "Amount per trade in quote asset", "number"),
                ("MAX_CANDIDATE_AGE_SECONDS", "Max candidate age seconds", "number"),
                ("BINANCE_BASE_URL", "Binance base URL", "text"),
                ("BINANCE_API_KEY", "Binance API key", "password"),
                ("BINANCE_SECRET_KEY", "Binance secret key", "password"),
                ("WEB_HOST", "Web host", "text"),
                ("WEB_PORT", "Web port", "number"),
                ("ADMIN_PASSWORD", "Admin password optional", "password"),
            ]
            for key, label, input_type in fields:
                value = values.get(key, "")
                body += f"<label>{escape(label)}</label><input name='{escape(key)}' type='{input_type}' value='{escape(value)}'>"
            body += "<button type='submit'>Save</button></form><p class='warn'>Restart the executor after changing API keys or trading settings.</p>"
            self._send("Admin", body)
            return
        values = public_env()
        rows = "".join(f"<p><b>{escape(k)}</b>: {escape(v)}</p>" for k, v in values.items())
        self._send("Raspberry Executor", f"<div class='box'><p class='ok'>Web UI online</p>{rows}</div>")

    def do_POST(self) -> None:
        if not self.path.startswith("/admin"):
            self._send("Not found", "Not found", 404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        payload = self.rfile.read(length).decode()
        posted = {key: values[-1] for key, values in parse_qs(payload).items()}
        current = read_env()
        for key, value in posted.items():
            if key in current:
                if key in SECRET_KEYS and value == "********":
                    continue
                current[key] = value.strip()
        write_env(current)
        self._redirect("/admin")

    def log_message(self, format, *args):
        return


def run_web(host: str = "0.0.0.0", port: int = 8090) -> None:
    server = ThreadingHTTPServer((host, port), AdminHandler)
    server.serve_forever()


if __name__ == "__main__":
    run_web()

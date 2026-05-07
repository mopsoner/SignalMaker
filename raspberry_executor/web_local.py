from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

from raspberry_executor.env_store import SECRET_KEYS, public_env, read_env, write_env
from raspberry_executor.logging_setup import LOG_FILE, tail_logs
from raspberry_executor.state import StateStore


def page(title, body):
    return f"""<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><meta http-equiv='refresh' content='20'><title>{escape(title)}</title><style>body{{font-family:Arial;margin:20px;background:#111;color:#eee}}a{{color:#8ab4ff}}nav a{{margin-right:14px}}.box{{background:#1b1b1b;padding:14px;border-radius:8px;margin:12px 0}}table{{width:100%;border-collapse:collapse}}th,td{{border-bottom:1px solid #333;padding:8px;text-align:left;font-size:14px}}th{{color:#aaa}}input{{width:100%;padding:10px;margin:6px 0 14px;background:#222;color:#eee;border:1px solid #444;box-sizing:border-box}}button,.button{{display:inline-block;padding:10px 16px;background:#2d6cdf;color:white!important;border:0;border-radius:6px;text-decoration:none;cursor:pointer}}button.danger,.button.danger{{background:#b42318}}pre{{background:#000;padding:12px;white-space:pre-wrap;overflow:auto}}.muted{{color:#aaa}}.ok{{color:#72e37b}}.warn{{color:#ffd166}}.actions{{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:12px 0}}</style></head><body><nav><a href='/'>Status</a><a href='/positions'>Positions</a><a href='/events'>Events</a><a href='/admin'>Admin</a><a href='/logs'>Logs</a></nav><h1>{escape(title)}</h1>{body}</body></html>""".encode()


def cell(v):
    return '' if v is None else escape(str(v))


def positions_html():
    state = StateStore()
    open_pos = state.open_positions()
    closed = list(reversed(state.closed_positions()[-50:]))
    body = "<div class='box'><h2>Open positions</h2>"
    body += positions_table(list(open_pos.items()))
    body += "</div><div class='box'><h2>Closed positions</h2>"
    body += positions_table([(p.get('candidate_id',''), p) for p in closed])
    body += "</div>"
    return body


def positions_table(rows):
    if not rows:
        return "<p class='muted'>No positions.</p>"
    out = "<table><tr><th>Status</th><th>Candidate</th><th>Signal</th><th>Execution</th><th>Side</th><th>Qty</th><th>Entry</th><th>Stop</th><th>Target</th><th>TP</th><th>SL</th><th>Reason</th></tr>"
    for cid, r in rows:
        out += '<tr>'
        for v in [r.get('status'), cid, r.get('signal_symbol'), r.get('execution_symbol'), r.get('side'), r.get('quantity'), r.get('entry_price'), r.get('stop_price'), r.get('target_price'), r.get('tp_order_id'), r.get('sl_order_id'), r.get('close_reason')]:
            out += f'<td>{cell(v)}</td>'
        out += '</tr>'
    return out + '</table>'


def events_html():
    rows = list(reversed(StateStore().events()[-200:]))
    if not rows:
        return "<div class='box'><p class='muted'>No local events.</p></div>"
    out = "<div class='box'><table><tr><th>Time</th><th>Candidate</th><th>Event</th><th>Payload</th></tr>"
    for r in rows:
        out += f"<tr><td>{cell(r.get('timestamp'))}</td><td>{cell(r.get('candidate_id'))}</td><td>{cell(r.get('event_type'))}</td><td>{cell(r.get('payload'))}</td></tr>"
    return out + '</table></div>'


def logs_actions_html():
    return """
    <div class='actions'>
      <a class='button' href='/logs/download'>Download full log file</a>
      <form method='post' action='/logs/delete' onsubmit="return confirm('Delete logs/executor.log ?');" style='margin:0'>
        <button class='danger' type='submit'>Delete log file</button>
      </form>
      <span class='muted'>File: logs/executor.log</span>
    </div>
    """


class Handler(BaseHTTPRequestHandler):
    def send_page(self, title, body, code=200):
        data = page(title, body)
        self.send_response(code)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def send_log_file(self):
        if not LOG_FILE.exists():
            data = b''
        else:
            data = LOG_FILE.read_bytes()
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Content-Disposition', 'attachment; filename="raspberry-executor.log"')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def delete_log_file(self):
        try:
            if LOG_FILE.exists():
                LOG_FILE.unlink()
        except Exception:
            # Keep the UI usable even if the file is currently locked by the logger.
            pass
        self.send_response(303)
        self.send_header('Location', '/logs')
        self.end_headers()

    def do_GET(self):
        if self.path.startswith('/positions'):
            return self.send_page('Local Positions', positions_html())
        if self.path.startswith('/events'):
            return self.send_page('Local Events', events_html())
        if self.path.startswith('/logs/download'):
            return self.send_log_file()
        if self.path.startswith('/logs'):
            logs = '\n'.join(escape(x) for x in tail_logs(400))
            return self.send_page('Logs', f"<div class='box'>{logs_actions_html()}<pre>{logs}</pre></div>")
        if self.path.startswith('/admin'):
            vals = read_env(); body = "<form method='post'>"
            for key in vals:
                t = 'password' if key in SECRET_KEYS else 'text'
                body += f"<label>{escape(key)}</label><input type='{t}' name='{escape(key)}' value='{escape(vals.get(key,''))}'>"
            body += "<button>Save</button></form><p class='warn'>Restart after changing trading settings.</p>"
            return self.send_page('Admin', body)
        rows = ''.join(f"<p><b>{escape(k)}</b>: {escape(v)}</p>" for k, v in public_env().items())
        return self.send_page('Raspberry Executor', f"<div class='box'><p class='ok'>Local mode: only trade candidates are read from SignalMaker.</p>{rows}</div>")

    def do_POST(self):
        if self.path.startswith('/logs/delete'):
            return self.delete_log_file()
        length = int(self.headers.get('Content-Length','0'))
        posted = {k: v[-1] for k, v in parse_qs(self.rfile.read(length).decode()).items()}
        current = read_env()
        for k, v in posted.items():
            if k in current and not (k in SECRET_KEYS and v == '********'):
                current[k] = v.strip()
        write_env(current)
        self.send_response(303); self.send_header('Location','/admin'); self.end_headers()

    def log_message(self, format, *args):
        return


def run_web(host='0.0.0.0', port=8090):
    ThreadingHTTPServer((host, port), Handler).serve_forever()

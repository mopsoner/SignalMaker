import json
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from urllib.parse import parse_qs, urlparse

from raspberry_executor.env_store import SECRET_KEYS, public_env, read_env, write_env
from raspberry_executor.logging_setup import LOG_FILE, tail_logs
from raspberry_executor.state import StateStore


def header_status_html():
    vals = read_env()
    remote = escape(vals.get('SIGNALMAKER_BASE_URL', 'not configured'))
    exchange = escape(vals.get('EXECUTION_EXCHANGE', vals.get('EXCHANGE', 'kraken/kraken')))
    candle = 'enabled' if vals.get('CANDLE_FEED_ENABLED', 'true').strip().lower() not in {'0', 'false', 'no', 'off'} else 'disabled'
    executor = 'dry-run' if vals.get('DRY_RUN', 'true').strip().lower() in {'1', 'true', 'yes', 'on'} else 'live'
    return f"<div class='box'><b>Remote SignalMaker URL:</b> {remote} &nbsp; <b>Local exchange:</b> {exchange} &nbsp; <b>Mode:</b> device executor &nbsp; <b>Candle feed status:</b> {candle} &nbsp; <b>Executor status:</b> {executor}</div>"


LEGACY_WARNING = "This Python http.server dashboard is deprecated. Use the FastAPI UI on port 8080: /index.html, /ops.html, /orders.html, or /momentum-candidates.html."


def official_ui_url(path='/index.html'):
    port = os.getenv('EXECUTOR_API_PORT', os.getenv('APP_PORT', '8080'))
    return f"http://{os.getenv('WEB_PUBLIC_HOST', '127.0.0.1')}:{port}{path}"


def page(title, body):
    header_status = header_status_html()
    warning = f"<div class='box warn'><b>Deprecated dashboard:</b> {escape(LEGACY_WARNING)}</div>"
    return f"""<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><meta http-equiv='refresh' content='20'><title>{escape(title)}</title><style>body{{font-family:Arial;margin:20px;background:#111;color:#eee}}a{{color:#8ab4ff}}nav a{{margin-right:14px}}.box{{background:#1b1b1b;padding:14px;border-radius:8px;margin:12px 0}}table{{width:100%;border-collapse:collapse}}th,td{{border-bottom:1px solid #333;padding:8px;text-align:left;font-size:14px}}th{{color:#aaa}}input{{width:100%;padding:10px;margin:6px 0 14px;background:#222;color:#eee;border:1px solid #444;box-sizing:border-box}}button,.button{{display:inline-block;padding:10px 16px;background:#2d6cdf;color:white!important;border:0;border-radius:6px;text-decoration:none;cursor:pointer}}button.danger,.button.danger{{background:#b42318}}pre{{background:#000;padding:12px;white-space:pre-wrap;overflow:auto}}.muted{{color:#aaa}}.ok{{color:#72e37b}}.warn{{color:#ffd166}}.actions{{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:12px 0}}</style></head><body><nav><a href='/index.html'>Status</a><a href='/ops.html'>Settings runtime</a><a href='/ops.html#logs'>Logs executor</a><a href='/positions.html'>Positions</a><a href='/candidates.html'>Candidates</a><a href='/momentum-candidates.html'>Momentum</a><a href='/ops.html#reset'>Reset local/runtime</a></nav><h1>SignalMaker Raspberry Executor</h1>{warning}{header_status}<h2>{escape(title)}</h2>{body}</body></html>""".encode()


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
    out = "<table><tr><th>Status</th><th>Candidate</th><th>Signal</th><th>Execution</th><th>Side</th><th>Qty</th><th>Entry</th><th>Target</th><th>TP</th><th>TP replay</th><th>Protected</th><th>Reason</th></tr>"
    for cid, r in rows:
        out += '<tr>'
        replay = r.get('tp_replay_status') or ('blocked' if r.get('tp_replay_blocked') else 'needed' if r.get('needs_tp_replay') else '')
        for v in [r.get('status'), cid, r.get('signal_symbol'), r.get('execution_symbol'), r.get('side'), r.get('quantity'), r.get('entry_price'), r.get('target_price'), r.get('tp_order_id'), replay, r.get('tp_protected_quantity'), r.get('close_reason')]:
            out += f'<td>{cell(v)}</td>'
        out += '</tr>'
    return out + '</table>'


def _event_limit(path="", default=200, maximum=1000):
    try:
        qs = parse_qs(urlparse(path).query)
        return min(max(1, int((qs.get("limit") or [default])[0])), maximum)
    except Exception:
        return default


def events_html(limit=200):
    rows = list(reversed(StateStore().events(limit=limit)))
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
    def _send_bytes(self, data, *, content_type, code=200):
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.send_header('Cache-Control', 'no-store, max-age=0')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)


    def redirect_official(self, path='/index.html'):
        self.send_response(308)
        self.send_header('Location', official_ui_url(path))
        self.end_headers()

    def send_page(self, title, body, code=200):
        self._send_bytes(page(title, body), content_type='text/html; charset=utf-8', code=code)

    def send_json(self, payload, code=200):
        data = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()
        self._send_bytes(data, content_type='application/json; charset=utf-8', code=code)

    def send_log_file(self):
        if not LOG_FILE.exists():
            data = b''
        else:
            data = LOG_FILE.read_bytes()
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Cache-Control', 'no-store, max-age=0')
        self.send_header('Pragma', 'no-cache')
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
        if self.path.startswith('/api/events'):
            limit = _event_limit(self.path, default=250, maximum=1000)
            return self.send_json({"events": list(reversed(StateStore().events(limit=limit))), "limit": limit})
        if self.path.startswith('/events'):
            return self.send_page('Local Events', events_html(limit=_event_limit(self.path)))
        redirect_map = {
            '/positions': '/positions.html',
            '/admin': '/ops.html',
            '/logs': '/ops.html',
            '/orders': '/orders.html',
            '/candidates': '/candidates.html',
            '/momentum': '/momentum-candidates.html',
            '/': '/index.html',
        }
        path = urlparse(self.path).path
        for legacy, target in redirect_map.items():
            if path == legacy or path.startswith(f'{legacy}/'):
                return self.redirect_official(target)
        return self.redirect_official('/index.html')

    def do_POST(self):
        self.send_response(308)
        self.send_header('Location', official_ui_url('/ops.html'))
        self.end_headers()

    def log_message(self, format, *args):
        return


def run_web(host='0.0.0.0', port=8090):
    ThreadingHTTPServer((host, port), Handler).serve_forever()

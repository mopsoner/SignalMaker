from html import escape
from http.server import ThreadingHTTPServer

from raspberry_executor.env_store import public_env
from raspberry_executor.logging_setup import tail_logs
from raspberry_executor.state import StateStore
from raspberry_executor.web_local import Handler as LocalHandler


def c(value):
    return '' if value is None else escape(str(value))


def dashboard():
    env = public_env()
    state = StateStore()
    open_count = len(state.open_positions())
    closed_count = len(state.closed_positions())
    events = state.events()
    error_count = len([e for e in events[-100:] if 'error' in str(e.get('event_type', '')).lower()])
    logs = '\n'.join(escape(x) for x in tail_logs(50))
    html = """
    <h1>Raspberry 360 Dashboard</h1>
    <div class='grid'>
      <div class='card'><b>Open positions</b><span>{open_count}</span></div>
      <div class='card'><b>Closed positions</b><span>{closed_count}</span></div>
      <div class='card'><b>Recent errors</b><span>{error_count}</span></div>
      <div class='card'><b>Quote assets</b><span>{quote}</span></div>
    </div>
    <div class='box'><h2>Status</h2>{status}</div>
    <div class='box'><h2>Config critique</h2>{config}</div>
    <div class='box'><h2>Actions</h2><a href='/positions'>Positions</a> | <a href='/events'>Events</a> | <a href='/logs'>Logs</a> | <a href='/admin'>Admin</a></div>
    <div class='box'><h2>Logs récents</h2><pre>{logs}</pre></div>
    """.format(
        open_count=open_count,
        closed_count=closed_count,
        error_count=error_count,
        quote=c(env.get('QUOTE_ASSETS')),
        status=' '.join([
            '<span class="pill">DRY_RUN={}</span>'.format(c(env.get('DRY_RUN'))),
            '<span class="pill">ALLOW_SHORTS={}</span>'.format(c(env.get('ALLOW_SHORTS'))),
            '<span class="pill">CANDLE_FEED_ENABLED={}</span>'.format(c(env.get('CANDLE_FEED_ENABLED'))),
            '<span class="pill">WORKERS={}</span>'.format(c(env.get('CANDLE_FEED_MAX_WORKERS'))),
        ]),
        config=''.join('<p><b>{}</b>: {}</p>'.format(c(k), c(env.get(k))) for k in ['SIGNALMAKER_BASE_URL','GATEWAY_ID','ORDER_QUOTE_AMOUNT','CANDLE_FEED_INTERVALS','CANDLE_FEED_POLL_SECONDS']),
        logs=logs,
    )
    return html


def event_message(event):
    event_type = str(event.get('event_type', ''))
    payload = event.get('payload') or {}
    text = str(payload)
    low = (event_type + ' ' + text).lower()
    if 'brokenpipeerror' in low or 'broken pipe' in low:
        return 'Browser/client disconnected while the UI was writing. Not a trading error.'
    if isinstance(payload, dict) and payload.get('error'):
        return str(payload.get('error'))[:220]
    if isinstance(payload, dict) and isinstance(payload.get('candidate'), dict):
        cand = payload.get('candidate')
        return '{} {} stop={} target={}'.format(cand.get('symbol',''), cand.get('side',''), cand.get('stop_price','-'), cand.get('target_price','-'))
    return text[:220]


def event_level(event):
    low = (str(event.get('event_type','')) + ' ' + str(event.get('payload') or {})).lower()
    if 'brokenpipeerror' in low or 'broken pipe' in low:
        return 'warn', 'UI warning'
    if 'error' in low or 'failed' in low or 'exception' in low:
        return 'bad', 'Error'
    if 'opened' in low or 'executed' in low or 'filled' in low:
        return 'ok', 'Trade'
    return '', 'Info'


def events_page():
    rows = list(reversed(StateStore().events()[-200:]))
    if not rows:
        return '<h1>Local Events</h1><div class="box"><p>No local events.</p></div>'
    html = '<h1>Local Events</h1><div class="box"><p class="muted">Readable history. BrokenPipe = browser disconnected, not a trade failure.</p><table><tr><th>Level</th><th>Time</th><th>Candidate</th><th>Event</th><th>Message</th></tr>'
    for row in rows:
        klass, label = event_level(row)
        html += '<tr><td><span class="pill {}">{}</span></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.format(klass, c(label), c(row.get('timestamp')), c(row.get('candidate_id')), c(row.get('event_type')), c(event_message(row)))
    return html + '</table></div>'


def page(body):
    head = """<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Raspberry 360</title><style>body{font-family:Arial;margin:0;background:#0b0f14;color:#eee}nav{background:#111923;padding:10px;white-space:nowrap;overflow:auto}nav a{color:#dce8ff;margin-right:14px;text-decoration:none}main{padding:12px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px}.card,.box{background:#151c26;border:1px solid #263241;border-radius:12px;padding:12px;margin:10px 0}.card b{display:block;color:#aebbd0;font-size:12px}.card span{font-size:24px;font-weight:800}.pill{display:inline-block;background:#263241;border-radius:999px;padding:5px 9px;margin:3px}.pill.ok{background:#12351c;color:#72e37b}.pill.warn{background:#3b2f0d;color:#ffd166}.pill.bad{background:#3b1515;color:#ff7b72}a{color:#8ab4ff}table{width:100%;border-collapse:collapse}th,td{border-bottom:1px solid #2a3545;padding:7px;text-align:left;font-size:13px;vertical-align:top}pre{white-space:pre-wrap;background:#05070a;padding:10px;border-radius:10px;overflow:auto;max-height:420px}.muted{color:#9aa7b8}</style></head><body><nav><a href='/'>Dashboard</a><a href='/positions'>Positions</a><a href='/events'>Events</a><a href='/admin'>Admin</a><a href='/logs'>Logs</a></nav><main>"""
    return (head + body + "</main></body></html>").encode()


class Handler(LocalHandler):
    def do_GET(self):
        if self.path == '/' or self.path.startswith('/?'):
            data = page(dashboard())
        elif self.path.startswith('/events'):
            data = page(events_page())
        else:
            return super().do_GET()
        try:
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            return


def run_web(host='0.0.0.0', port=8090):
    ThreadingHTTPServer((host, port), Handler).serve_forever()

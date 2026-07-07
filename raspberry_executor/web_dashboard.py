import json
from html import escape
from http.server import ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from raspberry_executor.env_store import ROOT, public_env, read_env
from raspberry_executor.feed_run_store import latest_feed_run, latest_feed_runs
from raspberry_executor.logging_setup import tail_logs
from raspberry_executor.state import StateStore
from raspberry_executor.web_local import Handler as LocalHandler

RETRY_PATH = ROOT / "raspberry_executor" / "candle_retry_queue.json"
BACKFILL_STATE_PATH = ROOT / "raspberry_executor" / "candle_backfill_4h_state.json"


def c(value):
    return "" if value is None else escape(str(value))


def _payload(event):
    payload = event.get("payload") or {}
    return payload if isinstance(payload, dict) else {"payload": payload}


def latest_momentum_event(events):
    for event in reversed(events):
        if event.get("event_type") == "momentum_decision":
            payload = _payload(event)
            return {"timestamp": event.get("timestamp"), **payload}
    return None


def momentum_events(events, limit=30):
    rows = []
    for event in reversed(events):
        event_type = str(event.get("event_type") or "")
        if event_type.startswith("momentum_"):
            payload = _payload(event)
            rows.append({"timestamp": event.get("timestamp"), "event_type": event_type, **payload})
            if len(rows) >= limit:
                break
    return rows


def _status_pill(status):
    status_text = str(status or "unknown")
    klass = "ok" if status_text in {"ok", "running", "BUY", "HOLD"} else "warn" if status_text in {"partial", "skipped", "disabled", "blocked", "WAIT", "SELL", "ROTATE"} else "bad" if status_text == "error" else ""
    return f"<span class='pill {klass}'>{c(status_text)}</span>"


def dashboard():
    env = public_env()
    state = StateStore()
    events = state.events(limit=1000)
    open_count = len(state.open_positions())
    closed_count = len(state.closed_positions())
    error_count = len([e for e in events[-100:] if any(x in str(e.get("event_type", "")).lower() for x in ["error", "failed", "insufficient"])])
    logs = "\n".join(escape(x) for x in tail_logs(50))
    feed = latest_feed_run() or {}
    momentum = latest_momentum_event(events) or {}
    status = " ".join([
        f"<span class='pill'>DRY_RUN={c(env.get('DRY_RUN'))}</span>",
        f"<span class='pill'>CANDLE_FEED={c(env.get('CANDLE_FEED_ENABLED'))}</span>",
        f"<span class='pill'>MOMENTUM={c(env.get('MOMENTUM_DECISION_ENABLED'))}</span>",
        f"<span class='pill'>MOM_EXEC={c(env.get('MOMENTUM_DECISION_EXECUTE_ENABLED'))}</span>",
        f"<span class='pill'>FEED_STATUS={c(feed.get('status') or 'never')}</span>",
    ])
    config = "".join(f"<p><b>{c(k)}</b>: {c(env.get(k))}</p>" for k in ["SIGNALMAKER_BASE_URL", "GATEWAY_ID", "ORDER_QUOTE_AMOUNT", "QUOTE_ASSETS", "CANDLE_FEED_MAX_SYMBOLS", "MOMENTUM_DECISION_POLL_SECONDS"])
    momentum_html = momentum_summary_box(momentum)
    return f"""
    <h1>SignalMaker Raspberry Executor</h1>
    <div class='box'><b>Connecté à :</b> <code>{c(env.get('SIGNALMAKER_BASE_URL'))}</code> · <b>Exchange local :</b> <code>{c(env.get('EXECUTION_EXCHANGE') or 'kraken')}</code> · <b>Mode :</b> <code>Device</code></div>
    <div class='grid'>
      <div class='card'><b>Open positions</b><span>{open_count}</span></div>
      <div class='card'><b>Closed positions</b><span>{closed_count}</span></div>
      <div class='card'><b>Recent warnings/errors</b><span>{error_count}</span></div>
      <div class='card'><b>Quote assets</b><span>{c(env.get('QUOTE_ASSETS'))}</span></div>
    </div>
    <div class='box'><h2>Status</h2>{status}</div>
    {momentum_html}
    <div class='box'><h2>Config critique</h2>{config}</div>
    <div class='box'><h2>Actions</h2>{nav_links()}</div>
    <div class='box'><h2>Logs récents</h2><pre>{logs}</pre></div>
    """


def nav_links():
    return " | ".join([
        "<a href='/device-status'>Device Status</a>",
        "<a href='/remote-signalmaker'>Remote SignalMaker</a>",
        "<a href='/candle-feed'>Candle Feed</a>",
        "<a href='/backfill'>Backfill</a>",
        "<a href='/candidates'>Trade Candidates</a>",
        "<a href='/momentum-decision'>Momentum Orders</a>",
        "<a href='/positions'>Positions</a>",
        "<a href='/orders'>Orders / Fills</a>",
        "<a href='/kraken-diagnostics'>Kraken Diagnostics</a>",
        "<a href='/admin'>Settings</a>",
        "<a href='/logs'>Logs</a>",
    ])


def momentum_summary_box(momentum):
    if not momentum:
        return "<div class='box'><h2>Momentum Decision</h2><p class='muted'>No momentum decision yet.</p></div>"
    decision = momentum.get("decision") or {}
    target = decision.get("target_asset") if isinstance(decision, dict) else None
    target = target if isinstance(target, dict) else {}
    return f"""
    <div class='box'>
      <h2>Momentum Decision</h2>
      <div class='grid'>
        <div class='card'><b>Decision action</b><span>{_status_pill(momentum.get('decision_action') or momentum.get('action'))}</span></div>
        <div class='card'><b>Symbol</b><span>{c(momentum.get('symbol'))}</span></div>
        <div class='card'><b>Target symbol</b><span>{c(momentum.get('target_symbol'))}</span></div>
        <div class='card'><b>Status</b><span>{_status_pill(momentum.get('status'))}</span></div>
        <div class='card'><b>Order IDs</b><span>{c(momentum.get('order_ids'))}</span></div>
        <div class='card'><b>Fill IDs</b><span>{c(momentum.get('fill_ids'))}</span></div>
      </div>
      <p><b>Last:</b> {c(momentum.get('timestamp'))}</p>
      <p><b>Next check:</b> {c(momentum.get('next_check_at'))}</p>
      <p><b>Reason:</b> {c(momentum.get('reason'))}</p>
      <p><b>Execution result:</b> {c(momentum.get('execution_result'))}</p>
      <p><a href='/momentum-decision'>Open momentum details</a></p>
    </div>
    """


def momentum_decision_page():
    events = StateStore().events(limit=1000)
    latest = latest_momentum_event(events) or {}
    rows = momentum_events(events, limit=50)
    html = "<h1>Momentum Decision</h1>" + momentum_summary_box(latest)
    html += "<div class='box'><h2>Settings</h2>"
    env = read_env()
    for key in ["MOMENTUM_DECISION_ENABLED", "MOMENTUM_DECISION_EXECUTE_ENABLED", "MOMENTUM_DECISION_POLL_SECONDS", "MOMENTUM_DECISION_CADENCE_HOURS", "MOMENTUM_DECISION_STARTING_CAPITAL", "MOMENTUM_DECISION_PATH", "MOMENTUM_DECISION_METHOD", "MOMENTUM_DECISION_USE_REMOTE_RUN_ONCE", "MOMENTUM_EXECUTOR_RUN_ONCE_PATH"]:
        html += f"<p><b>{c(key)}</b>: <code>{c(env.get(key))}</code></p>"
    html += "<p class='muted'>Modify these values from Admin, then restart the executor.</p></div>"
    if rows:
        html += "<div class='box'><h2>Recent momentum events</h2><table><tr><th>Time</th><th>Event</th><th>Decision action</th><th>Symbol</th><th>Target</th><th>Status</th><th>Orders</th><th>Fills</th><th>Result</th><th>Reason/Error</th><th>Details</th></tr>"
        for row in rows:
            details = json.dumps(row.get("decision") or row, ensure_ascii=False, sort_keys=True, indent=2)
            html += f"<tr><td>{c(row.get('timestamp'))}</td><td>{c(row.get('event_type'))}</td><td>{c(row.get('decision_action') or row.get('action'))}</td><td>{c(row.get('symbol'))}</td><td>{c(row.get('target_symbol'))}</td><td>{c(row.get('status'))}</td><td>{c(row.get('order_ids'))}</td><td>{c(row.get('fill_ids'))}</td><td>{c(row.get('execution_result'))}</td><td>{c(row.get('reason') or row.get('error'))}</td><td><details><summary>json</summary><pre>{c(details)}</pre></details></td></tr>"
        html += "</table></div>"
    return html


def _retry_queue():
    if not RETRY_PATH.exists():
        return {}
    try:
        data = json.loads(RETRY_PATH.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _simple_table(title, rows, columns, limit=80):
    if not rows:
        return f"<div class='box'><h2>{c(title)}</h2><p class='muted'>None</p></div>"
    html = f"<div class='box'><h2>{c(title)} <span class='muted'>showing {min(len(rows), limit)} / {len(rows)}</span></h2>"
    html += "<table><tr>" + "".join(f"<th>{c(col)}</th>" for col in columns) + "</tr>"
    for row in rows[:limit]:
        html += "<tr>" + "".join(f"<td>{c(row.get(col))}</td>" for col in columns) + "</tr>"
    return html + "</table></div>"


def candle_feed_page():
    env = read_env()
    latest = latest_feed_run() or {}
    summary = latest.get("summary") or {}
    runs = latest_feed_runs(limit=25)
    pushed = summary.get("pushed") or []
    skipped = summary.get("skipped") or []
    errors = summary.get("errors") or []
    retry_rows = list(_retry_queue().values())
    html = "<h1>Candle Feed</h1><p class='muted'>Raspberry demande la dernière candle au SignalMaker distant, récupère les candles manquantes chez Kraken/Kraken, puis pousse vers SignalMaker distant.</p><div class='grid'>"
    for label, value in [("Status", _status_pill(summary.get("status") or latest.get("status") or "never")), ("Last run", c(latest.get("timestamp") or "never")), ("Symbols", c(summary.get("symbol_count") or latest.get("symbol_count") or 0)), ("Pushed", c(len(pushed))), ("Skipped", c(len(skipped))), ("Errors", c(len(errors))), ("Retry queue", c(len(retry_rows))), ("Mode", c(summary.get("execution_mode") or ""))]:
        html += f"<div class='card'><b>{label}</b><span>{value}</span></div>"
    html += "</div><div class='box'><h2>Config</h2>"
    for key in ["SIGNALMAKER_BASE_URL", "QUOTE_ASSETS", "CANDLE_FEED_ENABLED", "CANDLE_FEED_INTERVALS", "CANDLE_FEED_LIMIT", "CANDLE_FEED_POLL_SECONDS", "CANDLE_FEED_MAX_SYMBOLS", "CANDLE_FEED_MAX_WORKERS", "CANDLE_FEED_KRAKEN_REQUESTS_PER_MINUTE", "KRAKEN_BASE_URL"]:
        html += f"<p><b>{c(key)}</b>: <code>{c(env.get(key))}</code></p>"
    html += "</div>"
    html += _simple_table("Last pushed", pushed, ["symbol", "interval", "count", "upserted", "was_retry"])
    html += _simple_table("Last skipped", skipped, ["symbol", "interval", "reason", "latest_close_time"])
    html += _simple_table("Last errors", errors, ["symbol", "interval", "error", "retry_queued"])
    html += _simple_table("Retry queue", retry_rows, ["symbol", "interval", "attempts", "last_error_at", "last_error"])
    history = [{"timestamp": r.get("timestamp"), "status": r.get("status"), "symbol_count": r.get("symbol_count"), "pushed_count": r.get("pushed_count"), "skipped_count": r.get("skipped_count"), "error_count": r.get("error_count"), "retry_queue_size": r.get("retry_queue_size")} for r in runs]
    html += _simple_table("Run history", history, ["timestamp", "status", "symbol_count", "pushed_count", "skipped_count", "error_count", "retry_queue_size"], limit=25)
    return html



def _backfill_state():
    if not BACKFILL_STATE_PATH.exists():
        return {"symbols": {}, "runs": []}
    try:
        data = json.loads(BACKFILL_STATE_PATH.read_text())
        return data if isinstance(data, dict) else {"symbols": {}, "runs": []}
    except Exception:
        return {"symbols": {}, "runs": []}


def backfill_page():
    env = read_env()
    state = _backfill_state()
    runs = list(reversed(state.get("runs") or []))
    symbols = state.get("symbols") or {}
    symbol_rows = [{"symbol": k, **(v if isinstance(v, dict) else {})} for k, v in symbols.items()]
    latest = runs[0] if runs else {}
    html = "<h1>Backfill</h1><p class='muted'>Backfill historique device: SignalMaker distant expose/stocke le dernier état, le Raspberry récupère des candles historiques chez l'exchange local puis les pousse vers le SignalMaker distant. Lancer avec <code>./run.sh backfill</code>.</p>"
    html += "<div class='grid'>"
    for label, value in [("Status", _status_pill(latest.get("status") or "never")), ("Dernier run", c(latest.get("completed_at") or "never")), ("Pushed", c(latest.get("pushed") or 0)), ("Chunks", c(latest.get("chunks") or 0)), ("Exchange", c(env.get("EXECUTION_EXCHANGE") or "kraken")), ("Quote assets", c(env.get("QUOTE_ASSETS")))]:
        html += f"<div class='card'><b>{label}</b><span>{value}</span></div>"
    html += "</div><div class='box'><h2>Config</h2>"
    for key in ["SIGNALMAKER_BASE_URL", "EXECUTION_EXCHANGE", "QUOTE_ASSETS", "BACKFILL_4H_ENABLED", "BACKFILL_4H_DAYS", "BACKFILL_4H_MAX_SYMBOLS_PER_RUN", "BACKFILL_4H_MAX_CHUNKS_PER_SYMBOL", "BACKFILL_4H_CHUNK_LIMIT"]:
        html += f"<p><b>{c(key)}</b>: <code>{c(env.get(key))}</code></p>"
    html += "</div>"
    html += _simple_table("Symbol status", symbol_rows, ["symbol", "status", "next_start_time", "last_close_time_sent", "last_sent_at", "last_error"], limit=100)
    html += _simple_table("Run history", runs, ["completed_at", "status", "interval", "days", "symbols_requested", "pushed", "chunks", "execution_mode", "exchange"], limit=25)
    return html

def raw_payload(event):
    return str(event.get("payload") or {})


def event_level(event):
    event_type = str(event.get("event_type", ""))
    text = (event_type + " " + raw_payload(event)).lower()
    if event_type in {"position_opened", "tp_replayed", "tp_existing_order_attached", "take_profit_filled", "momentum_bought", "momentum_sold"}:
        return "ok", "OK"
    if event_type in {"tp_replay_waiting_levels", "tp_replay_blocked", "margin_skipped_insufficient_balance"} or "skipped" in event_type:
        return "warn", "Watch"
    if any(x in text for x in ["error", "failed", "insufficient", "rejected", "invalid_oco", "invalid_take_profit"]):
        return "bad", "Error"
    return "", "Info"


def event_message(event):
    payload = _payload(event)
    if str(event.get("event_type") or "").startswith("momentum_"):
        return "action={} symbol={} buy={} sell={} result={} reason={}".format(payload.get("action"), payload.get("symbol"), payload.get("buy_symbol"), payload.get("sell_symbol"), payload.get("execution_result"), payload.get("reason") or payload.get("error"))
    if isinstance(payload, dict) and payload.get("error"):
        return str(payload.get("error"))[:500]
    return raw_payload(event)[:500]


def payload_table(event):
    payload = _payload(event)
    if not payload:
        return ""
    return f"<details><summary>Payload details</summary><pre>{c(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))}</pre></details>"


def _request_limit(path: str, *, default: int = 250, maximum: int = 1000) -> int:
    try:
        qs = parse_qs(urlparse(path).query)
        return min(max(1, int((qs.get("limit") or [default])[0])), maximum)
    except Exception:
        return default


def events_page(limit: int = 250):
    rows = list(reversed(StateStore().events(limit=limit)))
    if not rows:
        return "<h1>Local Events</h1><div class='box'><p>No local events.</p></div>"
    html = "<h1>Local Events</h1><div class='box'><table><tr><th>Level</th><th>Time</th><th>Candidate</th><th>Event</th><th>Message</th><th>Details</th></tr>"
    for row in rows:
        klass, label = event_level(row)
        html += "<tr><td><span class='pill {}'>{}</span></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(klass, c(label), c(row.get("timestamp")), c(row.get("candidate_id")), c(row.get("event_type")), c(event_message(row)), payload_table(row))
    return html + "</table></div>"


def page(body):
    head = """<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><meta http-equiv='refresh' content='10'><title>SignalMaker Raspberry Executor</title><style>body{font-family:Arial;margin:0;background:#0b0f14;color:#eee}nav{background:#111923;padding:10px;white-space:nowrap;overflow:auto}nav a{color:#dce8ff;margin-right:14px;text-decoration:none}main{padding:12px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px}.card,.box{background:#151c26;border:1px solid #263241;border-radius:12px;padding:12px;margin:10px 0}.card b{display:block;color:#aebbd0;font-size:12px}.card span{font-size:24px;font-weight:800}.pill{display:inline-block;background:#263241;border-radius:999px;padding:5px 9px;margin:3px}.pill.ok{background:#12351c;color:#72e37b}.pill.warn{background:#3b2f0d;color:#ffd166}.pill.bad{background:#3b1515;color:#ff7b72}a{color:#8ab4ff}table{width:100%;border-collapse:collapse}th,td{border-bottom:1px solid #2a3545;padding:7px;text-align:left;font-size:13px;vertical-align:top}code{color:#dce8ff;word-break:break-word}details summary{cursor:pointer;color:#8ab4ff}pre{white-space:pre-wrap;background:#05070a;padding:10px;border-radius:10px;overflow:auto;max-height:420px}.muted{color:#9aa7b8}</style></head><body><nav><a href='/'>Device Status</a><a href='/remote-signalmaker'>Remote SignalMaker</a><a href='/candle-feed'>Candle Feed</a><a href='/backfill'>Backfill</a><a href='/candidates'>Trade Candidates</a><a href='/momentum-decision'>Momentum Orders</a><a href='/positions'>Positions</a><a href='/orders'>Orders / Fills</a><a href='/kraken-diagnostics'>Kraken Diagnostics</a><a href='/admin'>Settings</a><a href='/logs'>Logs</a></nav><main>"""
    return (head + body + "</main></body></html>").encode()


class Handler(LocalHandler):
    def do_GET(self):
        if self.path.startswith("/api/events"):
            limit = _request_limit(self.path, default=250, maximum=1000)
            return self.send_json({"events": list(reversed(StateStore().events(limit=limit))), "limit": limit})
        if self.path == "/" or self.path.startswith("/?"):
            data = page(dashboard())
        elif self.path.startswith("/candle-feed"):
            data = page(candle_feed_page())
        elif self.path.startswith("/backfill"):
            data = page(backfill_page())
        elif self.path.startswith("/device-status") or self.path.startswith("/remote-signalmaker") or self.path.startswith("/kraken-diagnostics"):
            data = page(dashboard())
        elif self.path.startswith("/momentum-decision"):
            data = page(momentum_decision_page())
        elif self.path.startswith("/events"):
            data = page(events_page(limit=_request_limit(self.path)))
        else:
            return super().do_GET()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            return


def run_web(host="0.0.0.0", port=8090):
    ThreadingHTTPServer((host, port), Handler).serve_forever()

import json
from html import escape
from http.server import ThreadingHTTPServer

from raspberry_executor.env_store import ROOT, public_env, read_env
from raspberry_executor.feed_run_store import latest_feed_run, latest_feed_runs
from raspberry_executor.logging_setup import tail_logs
from raspberry_executor.state import StateStore
from raspberry_executor.web_local import Handler as LocalHandler

RETRY_PATH = ROOT / "raspberry_executor" / "candle_retry_queue.json"


def c(value):
    return "" if value is None else escape(str(value))


def dashboard():
    env = public_env()
    state = StateStore()
    open_count = len(state.open_positions())
    closed_count = len(state.closed_positions())
    events = state.events()
    error_count = len([e for e in events[-100:] if any(x in str(e.get("event_type", "")).lower() for x in ["error", "failed", "insufficient"])])
    logs = "\n".join(escape(x) for x in tail_logs(50))
    feed = latest_feed_run() or {}
    status = " ".join([
        f"<span class='pill'>DRY_RUN={c(env.get('DRY_RUN'))}</span>",
        f"<span class='pill'>CANDLE_FEED_ENABLED={c(env.get('CANDLE_FEED_ENABLED'))}</span>",
        f"<span class='pill'>WORKERS={c(env.get('CANDLE_FEED_MAX_WORKERS'))}</span>",
        f"<span class='pill'>FEED_STATUS={c(feed.get('status') or 'never')}</span>",
    ])
    config = "".join(
        f"<p><b>{c(k)}</b>: {c(env.get(k))}</p>"
        for k in ["SIGNALMAKER_BASE_URL", "GATEWAY_ID", "ORDER_QUOTE_AMOUNT", "QUOTE_ASSETS", "CANDLE_FEED_INTERVALS", "CANDLE_FEED_POLL_SECONDS", "CANDLE_FEED_MAX_SYMBOLS"]
    )
    return f"""
    <h1>Raspberry 360 Dashboard</h1>
    <div class='grid'>
      <div class='card'><b>Open positions</b><span>{open_count}</span></div>
      <div class='card'><b>Closed positions</b><span>{closed_count}</span></div>
      <div class='card'><b>Recent warnings/errors</b><span>{error_count}</span></div>
      <div class='card'><b>Quote assets</b><span>{c(env.get('QUOTE_ASSETS'))}</span></div>
    </div>
    <div class='box'><h2>Status</h2>{status}</div>
    <div class='box'><h2>Config critique</h2>{config}</div>
    <div class='box'><h2>Actions</h2>{nav_links()}</div>
    <div class='box'><h2>Logs récents</h2><pre>{logs}</pre></div>
    """


def nav_links():
    return " | ".join([
        "<a href='/positions'>Positions</a>",
        "<a href='/candidates'>Candidates</a>",
        "<a href='/candle-feed'>Candle Feed</a>",
        "<a href='/events'>Events</a>",
        "<a href='/logs'>Logs</a>",
        "<a href='/admin'>Admin</a>",
    ])


def _status_pill(status):
    status_text = str(status or "unknown")
    klass = "ok" if status_text == "ok" else "warn" if status_text in {"partial", "skipped", "blocked"} else "bad" if status_text == "error" else ""
    return f"<span class='pill {klass}'>{c(status_text)}</span>"


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
    retry = _retry_queue()
    retry_rows = list(retry.values()) if isinstance(retry, dict) else []
    cards = [
        ("Status", _status_pill(summary.get("status") or latest.get("status") or "never")),
        ("Last run", c(latest.get("timestamp") or "never")),
        ("Symbols", c(summary.get("symbol_count") or latest.get("symbol_count") or 0)),
        ("Pushed", c(len(pushed))),
        ("Skipped", c(len(skipped))),
        ("Errors", c(len(errors))),
        ("Retry queue", c(len(retry_rows))),
        ("Mode", c(summary.get("execution_mode") or "")),
    ]
    html = "<h1>Candle Feed</h1><div class='grid'>"
    for label, value in cards:
        html += f"<div class='card'><b>{label}</b><span>{value}</span></div>"
    html += "</div>"
    html += "<div class='box'><h2>Config</h2>"
    for key in ["SIGNALMAKER_BASE_URL", "QUOTE_ASSETS", "CANDLE_FEED_ENABLED", "CANDLE_FEED_INTERVALS", "CANDLE_FEED_LIMIT", "CANDLE_FEED_POLL_SECONDS", "CANDLE_FEED_MAX_SYMBOLS", "CANDLE_FEED_MAX_WORKERS", "CANDLE_FEED_BINANCE_REQUESTS_PER_MINUTE", "BINANCE_BASE_URL"]:
        html += f"<p><b>{c(key)}</b>: <code>{c(env.get(key))}</code></p>"
    html += "<p class='muted'>Partial = at least one pair failed. Check Last errors and Retry queue below.</p></div>"
    if summary.get("endpoint_check"):
        html += f"<div class='box'><h2>Endpoint check</h2><pre>{c(json.dumps(summary.get('endpoint_check'), ensure_ascii=False, sort_keys=True, indent=2))}</pre></div>"
    html += _simple_table("Last pushed", pushed, ["symbol", "interval", "count", "upserted", "was_retry"])
    html += _simple_table("Last skipped", skipped, ["symbol", "interval", "reason", "latest_close_time"])
    html += _simple_table("Last errors", errors, ["symbol", "interval", "error", "retry_queued"])
    html += _simple_table("Retry queue", retry_rows, ["symbol", "interval", "attempts", "last_error_at", "last_error"])
    history = []
    for run in runs:
        history.append({
            "timestamp": run.get("timestamp"),
            "status": run.get("status"),
            "symbol_count": run.get("symbol_count"),
            "pushed_count": run.get("pushed_count"),
            "skipped_count": run.get("skipped_count"),
            "error_count": run.get("error_count"),
            "retry_queue_size": run.get("retry_queue_size"),
        })
    html += _simple_table("Run history", history, ["timestamp", "status", "symbol_count", "pushed_count", "skipped_count", "error_count", "retry_queue_size"], limit=25)
    return html


def raw_payload(event):
    return str(event.get("payload") or {})


def _payload(event):
    payload = event.get("payload") or {}
    return payload if isinstance(payload, dict) else {"payload": payload}


def _candidate_from_payload(payload: dict) -> dict:
    candidate = payload.get("candidate")
    return candidate if isinstance(candidate, dict) else {}


def event_level(event):
    event_type = str(event.get("event_type", ""))
    text = (event_type + " " + raw_payload(event)).lower()
    if event_type in {"position_opened", "oco_repaired", "take_profit_filled"}:
        return "ok", "OK"
    if event_type in {"stop_loss_filled", "oco_repair_waiting_levels", "margin_skipped_insufficient_balance"}:
        return "warn", "Watch"
    if "brokenpipeerror" in text or "broken pipe" in text:
        return "warn", "UI warning"
    if any(x in text for x in ["error", "failed", "insufficient", "rejected", "invalid_oco"]):
        return "bad", "Error"
    return "", "Info"


def _compact_details(event):
    payload = _payload(event)
    candidate = _candidate_from_payload(payload)
    levels = payload.get("oco_repair_level_source") or payload.get("levels") or {}
    parts = []
    keys = [("symbol", "sym"), ("execution_symbol", "sym"), ("signal_symbol", "sig"), ("side", "side"), ("mode", "mode"), ("reason", "reason"), ("error", "error"), ("quantity", "qty"), ("entry_price", "entry"), ("target_price", "target"), ("stop_price", "stop"), ("oco_order_list_id", "oco"), ("tp_order_id", "tp_id"), ("sl_order_id", "sl_id"), ("oco_repair_mode", "repair"), ("order_monitor_mode", "monitor")]
    for key, label in keys:
        value = payload.get(key)
        if value is not None and value != "":
            parts.append(f"{label}={value}")
    for key, label in [("symbol", "cand_sym"), ("side", "cand_side"), ("status", "cand_status"), ("stop_price", "cand_stop"), ("target_price", "cand_target")]:
        value = candidate.get(key)
        if value is not None and value != "":
            parts.append(f"{label}={value}")
    if isinstance(levels, dict):
        source = levels.get("source") or levels.get("source_candidate_id")
        if source:
            parts.append(f"level_source={source}")
    return " | ".join(parts)


def event_message(event):
    event_type = str(event.get("event_type", ""))
    payload = _payload(event)
    text = raw_payload(event)
    low = (event_type + " " + text).lower()
    details = _compact_details(event)
    if event_type == "position_opened" and isinstance(payload, dict):
        return details or "Opened {} {} qty={} entry={} stop={} target={}".format(payload.get("execution_symbol", ""), payload.get("side", ""), payload.get("quantity", "-"), payload.get("entry_price", "-"), payload.get("stop_price", "-"), payload.get("target_price", "-"))
    if event_type == "oco_repaired":
        return details or "OCO repaired."
    if event_type == "oco_repair_waiting_levels":
        return details or "Waiting for stop/target levels."
    if event_type == "margin_skipped_insufficient_balance":
        return details or "Margin skipped: insufficient available balance."
    if event_type == "stop_loss_filled":
        return details or "Stop loss filled. Position closed by Binance OCO."
    if event_type == "take_profit_filled":
        return details or "Take profit filled. Position closed by Binance OCO."
    if "brokenpipeerror" in low or "broken pipe" in low:
        return "Browser/client disconnected while the UI was writing. Not a trading error."
    if "insufficient balance" in low:
        return details or "Insufficient balance. Binance refused the order because available balance was too low or reserved."
    if "invalid_oco_price_order" in low:
        return details or "OCO rejected before submit: target/current/stop order is no longer valid."
    if isinstance(payload, dict) and payload.get("error"):
        return details or str(payload.get("error"))[:500]
    if details:
        return details
    return text[:500]


def payload_table(event):
    payload = _payload(event)
    if not payload:
        return ""
    rows = []
    for key in ["symbol", "execution_symbol", "signal_symbol", "side", "mode", "reason", "error", "quantity", "entry_price", "target_price", "stop_price", "oco_order_list_id", "tp_order_id", "sl_order_id", "oco_repair_mode", "order_monitor_mode"]:
        value = payload.get(key)
        if value is not None and value != "":
            rows.append(f"<tr><td>{c(key)}</td><td><code>{c(value)}</code></td></tr>")
    candidate = _candidate_from_payload(payload)
    if candidate:
        rows.append(f"<tr><td>candidate</td><td><code>{c(json.dumps(candidate, ensure_ascii=False, sort_keys=True))}</code></td></tr>")
    levels = payload.get("oco_repair_level_source") or payload.get("levels")
    if levels:
        rows.append(f"<tr><td>levels</td><td><code>{c(json.dumps(levels, ensure_ascii=False, sort_keys=True))}</code></td></tr>")
    if not rows:
        rows.append(f"<tr><td>payload</td><td><code>{c(json.dumps(payload, ensure_ascii=False, sort_keys=True))}</code></td></tr>")
    return "<details><summary>Payload details</summary><table class='mini'>" + "".join(rows) + "</table></details>"


def events_page():
    rows = list(reversed(StateStore().events()[-250:]))
    if not rows:
        return "<h1>Local Events</h1><div class='box'><p>No local events.</p></div>"
    html = "<h1>Local Events</h1><div class='box'><p class='muted'>Readable history with payload details. BrokenPipe = browser disconnected, not a trade failure.</p><table><tr><th>Level</th><th>Time</th><th>Candidate</th><th>Event</th><th>Message</th><th>Details</th></tr>"
    for row in rows:
        klass, label = event_level(row)
        html += "<tr><td><span class='pill {}'>{}</span></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(klass, c(label), c(row.get("timestamp")), c(row.get("candidate_id")), c(row.get("event_type")), c(event_message(row)), payload_table(row))
    return html + "</table></div>"


def page(body):
    head = """<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Raspberry 360</title><style>body{font-family:Arial;margin:0;background:#0b0f14;color:#eee}nav{background:#111923;padding:10px;white-space:nowrap;overflow:auto}nav a{color:#dce8ff;margin-right:14px;text-decoration:none}main{padding:12px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px}.card,.box{background:#151c26;border:1px solid #263241;border-radius:12px;padding:12px;margin:10px 0}.card b{display:block;color:#aebbd0;font-size:12px}.card span{font-size:24px;font-weight:800}.pill{display:inline-block;background:#263241;border-radius:999px;padding:5px 9px;margin:3px}.pill.ok{background:#12351c;color:#72e37b}.pill.warn{background:#3b2f0d;color:#ffd166}.pill.bad{background:#3b1515;color:#ff7b72}a{color:#8ab4ff}table{width:100%;border-collapse:collapse}th,td{border-bottom:1px solid #2a3545;padding:7px;text-align:left;font-size:13px;vertical-align:top}table.mini td{font-size:12px;padding:4px}code{color:#dce8ff;word-break:break-word}details summary{cursor:pointer;color:#8ab4ff}pre{white-space:pre-wrap;background:#05070a;padding:10px;border-radius:10px;overflow:auto;max-height:420px}.muted{color:#9aa7b8}</style></head><body><nav><a href='/'>Dashboard</a><a href='/positions'>Positions</a><a href='/candidates'>Candidates</a><a href='/candle-feed'>Candle Feed</a><a href='/events'>Events</a><a href='/admin'>Admin</a><a href='/logs'>Logs</a></nav><main>"""
    return (head + body + "</main></body></html>").encode()


class Handler(LocalHandler):
    def do_GET(self):
        if self.path == "/" or self.path.startswith("/?"):
            data = page(dashboard())
        elif self.path.startswith("/candle-feed"):
            data = page(candle_feed_page())
        elif self.path.startswith("/events"):
            data = page(events_page())
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


def run_web(host="0.0.0.0", port=8090):
    ThreadingHTTPServer((host, port), Handler).serve_forever()

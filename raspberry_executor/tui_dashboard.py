import curses
import json
import os
import time
from datetime import datetime
from types import SimpleNamespace

from raspberry_executor.margin_settings import execution_mode, margin_dry_run, margin_multiplier, shorts_enabled
from raspberry_executor.tui_api import BASE_URL, api_get, as_rows
from raspberry_executor.ui_contract import market_candles_summary_view, momentum_view

REFRESH_SECONDS = 5


def candidate_display_limit() -> int:
    try:
        return max(1, int(os.getenv("TUI_CANDIDATE_LIMIT", os.getenv("CANDIDATE_FETCH_LIMIT", "50")) or "50"))
    except Exception:
        return 50


def safe(value, default="-"):
    if value is None:
        return default
    text = str(value)
    return text if text else default


def trunc(value, width):
    text = safe(value, "")
    return text if len(text) <= width else text[: max(0, width - 1)] + "…"


def _float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def parse_dt(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def fr_datetime(value, *, with_date: bool = True) -> str:
    dt = parse_dt(value)
    if not dt:
        return safe(value)
    if dt.tzinfo is not None:
        dt = dt.astimezone()
    return dt.strftime("%d/%m %H:%M:%S") if with_date else dt.strftime("%H:%M:%S")


def candidate_received_at(candidate: dict) -> str:
    return fr_datetime(candidate.get("received_at") or candidate.get("created_at") or candidate.get("updated_at") or candidate.get("timestamp") or candidate.get("exported_at"))


def add(stdscr, y, x, text, attr=0):
    h, w = stdscr.getmaxyx()
    if y < 0 or y >= h or x >= w:
        return
    try:
        stdscr.addstr(y, x, str(text)[: max(0, w - x - 1)], attr)
    except curses.error:
        pass


def box(stdscr, y, x, h, w, title=""):
    max_h, max_w = stdscr.getmaxyx()
    if y >= max_h or x >= max_w or h < 3 or w < 8:
        return
    h = min(h, max_h - y)
    w = min(w, max_w - x)
    add(stdscr, y, x, "┌" + "─" * (w - 2) + "┐", curses.color_pair(4))
    for row in range(y + 1, y + h - 1):
        add(stdscr, row, x, "│", curses.color_pair(4))
        add(stdscr, row, x + w - 1, "│", curses.color_pair(4))
    add(stdscr, y + h - 1, x, "└" + "─" * (w - 2) + "┘", curses.color_pair(4))
    if title:
        add(stdscr, y, x + 2, f" {title} ", curses.color_pair(3) | curses.A_BOLD)


def _payload(event: dict) -> dict:
    payload = event.get("payload") or {}
    return payload if isinstance(payload, dict) else {"payload": payload}


def latest_momentum_decision(events: list[dict]) -> dict | None:
    for event in reversed(events):
        if event.get("event_type") == "momentum_decision":
            payload = _payload(event)
            return {"timestamp": event.get("timestamp"), **payload}
    return None


def momentum_candidate_rows(candidates: list[dict]) -> list[dict]:
    return [row for row in candidates if str(row.get("stage") or "").lower() == "momentum" or str((row.get("payload") or {}).get("source") or "").startswith("momentum")]


def candidate_rsi_1h(row: dict):
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
    remote = payload.get("remote_candidate") if isinstance(payload.get("remote_candidate"), dict) else {}
    derived = payload.get("momentum_trade_candidate") if isinstance(payload.get("momentum_trade_candidate"), dict) else {}
    return row.get("rsi_1h") or remote.get("rsi_1h") or derived.get("rsi_1h")


def momentum_buyable_text(row: dict) -> str:
    try:
        rsi = float(candidate_rsi_1h(row))
    except Exception:
        return "no-rsi"
    return "buy" if 50 <= rsi <= 65 else "blocked"



def summarize_candle_feed_rows(rows: list[dict]) -> dict:
    latest = None
    for row in rows:
        value = row.get("latest_open_time") or row.get("last_open_time") or row.get("open_time") or row.get("updated_at") or row.get("timestamp")
        dt = parse_dt(value)
        if dt and (latest is None or dt > latest):
            latest = dt
    symbols = [safe(row.get("symbol")) for row in rows[:5]]
    return {"ok": True, "count": len(rows), "latest": latest.isoformat() if latest else None, "symbols": symbols}


def settings_namespace(payload: dict) -> SimpleNamespace:
    executor = payload.get("executor", {}) if isinstance(payload, dict) else {}
    live = payload.get("live", {}) if isinstance(payload, dict) else {}
    market_data = payload.get("market_data", {}) if isinstance(payload, dict) else {}
    strategy = payload.get("strategy", {}) if isinstance(payload, dict) else {}
    quote_assets = executor.get("quote_assets") or market_data.get("kraken_quote_assets") or []
    if isinstance(quote_assets, str):
        quote_assets = [item.strip() for item in quote_assets.split(",") if item.strip()]
    return SimpleNamespace(
        raw=payload,
        order_quote_amount=executor.get("order_quote_amount") or strategy.get("order_quote_amount") or live.get("order_quote_amount") or "-",
        quote_assets=quote_assets,
        dry_run=not bool(live.get("live_trading_enabled")),
    )


def position_strategy(candidate_id: str, row: dict) -> str:
    meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
    if str(candidate_id).startswith("momentum-") or str(row.get("stage") or meta.get("stage") or "").lower() == "momentum":
        return "mom"
    mode = str(row.get("mode") or meta.get("mode") or "").lower()
    if mode == "margin":
        return "mgn"
    return "sig"


def normalize_position(row: dict) -> tuple[str, dict]:
    meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
    candidate_id = str(meta.get("candidate_id") or row.get("candidate_id") or row.get("position_id") or "-")
    pos = dict(row)
    pos.setdefault("candidate_id", candidate_id)
    pos.setdefault("execution_symbol", row.get("symbol"))
    pos.setdefault("signal_symbol", row.get("symbol"))
    pos.setdefault("target_price", row.get("target_price") or row.get("stop_price"))
    pos.setdefault("pnl", row.get("unrealized_pnl"))
    pos.setdefault("strategy_label", position_strategy(candidate_id, pos))
    return candidate_id, pos


def latest_momentum_row(rows: list[dict]) -> dict | None:
    if not rows:
        return None
    row = rows[0]
    return {
        "decision_action": row.get("decision_action") or row.get("action") or row.get("status"),
        "symbol": row.get("symbol"),
        "target_symbol": row.get("target_symbol") or row.get("symbol"),
        "status": row.get("status"),
        "reason": row.get("notes") or row.get("reason"),
        "order_ids": row.get("order_ids"),
        "fill_ids": row.get("fill_ids"),
        "best_asset.symbol": row.get("symbol"),
        "best_asset.price": row.get("entry_price"),
        "best_asset.momentum_score": row.get("score"),
    }


def snapshot():
    limit = candidate_display_limit()
    now = datetime.now().astimezone()
    errors: dict[str, str] = {}

    def get(path: str, params: dict | None = None, default=None):
        try:
            return api_get(path, params)
        except Exception as exc:
            errors[path] = str(exc)
            return default

    health = get("/healthz", default={})
    settings_payload = get("/api/v1/admin/settings", default={})
    candidates = as_rows(get("/api/v1/trade-candidates", {"limit": limit}, default=[]))
    positions = [normalize_position(row) for row in as_rows(get("/api/v1/positions", {"limit": 100}, default=[])) if isinstance(row, dict)]
    momentum_contract = momentum_view(get("/api/v1/momentum", {"limit": 100}, default=[]), [errors["/api/v1/momentum"]] if "/api/v1/momentum" in errors else None)
    momentum_candidates = momentum_contract["rows"]
    candle_contract = market_candles_summary_view(get("/api/v1/market-data/candles/summary", default=[]), [errors["/api/v1/market-data/candles/summary"]] if "/api/v1/market-data/candles/summary" in errors else None)
    candle_rows = [row for row in candle_contract["rows"] if isinstance(row, dict)]
    logs_payload = get("/api/v1/admin/logs/executor", {"lines": 160}, default={})
    log_lines = logs_payload.get("lines", []) if isinstance(logs_payload, dict) else []
    events = [{"timestamp": None, "candidate_id": "executor", "event_type": "log", "payload": {"message": line}} for line in reversed(log_lines)]
    pnl_values = [row.get("pnl") for _, row in positions if row.get("pnl") is not None]
    total_pnl = sum(_float(value) for value in pnl_values) if pnl_values else None
    return {
        "base_url": BASE_URL,
        "health": health,
        "settings": settings_namespace(settings_payload if isinstance(settings_payload, dict) else {}),
        "execution_mode": execution_mode(),
        "margin_dry_run": margin_dry_run(),
        "margin_multiplier": margin_multiplier(),
        "shorts_enabled": shorts_enabled(),
        "positions": positions,
        "pnl_summary": {"total_pnl": total_pnl, "total_ok": "/api/v1/positions" not in errors},
        "events": events,
        "momentum": latest_momentum_row([r for r in momentum_candidates if isinstance(r, dict)]),
        "momentum_candidates": momentum_candidates,
        "momentum_contract": momentum_contract,
        "candidates": candidates,
        "candidate_error": errors.get("/api/v1/trade-candidates"),
        "candle_feed": summarize_candle_feed_rows(candle_rows),
        "candle_contract": candle_contract,
        "candle_error": errors.get("/api/v1/market-data/candles/summary"),
        "candidate_limit": limit,
        "refreshed_at": now.strftime("%d/%m %H:%M:%S"),
        "errors": errors,
    }



def target_reached(row: dict) -> bool:
    mark = row.get("mark_price")
    target = row.get("target_price")
    try:
        mark_value = float(mark)
        target_value = float(target)
    except Exception:
        return False
    side = str(row.get("side") or "").lower()
    return mark_value <= target_value if side == "short" else mark_value >= target_value


def position_result(row: dict) -> str:
    if str(row.get("status") or "").lower() == "closed":
        return "Closed"
    if not row.get("tp_order_id"):
        if row.get("tp_replay_blocked"):
            return "TP bloqué"
        if row.get("needs_tp_replay") or row.get("last_tp_replay_skip_reason"):
            return "Rejeu TP"
        return "Sans TP"
    replay_status = str(row.get("tp_replay_status") or "")
    if replay_status == "partial_placed":
        return "TP partiel"
    if target_reached(row):
        return "Gagnante TP"
    return "Attente TP"


def render_header(stdscr, width, data):
    add(stdscr, 0, 0, " " * (width - 1), curses.color_pair(1))
    add(stdscr, 0, 2, " SignalMaker Raspberry Executor — Overview ", curses.color_pair(1) | curses.A_BOLD)
    right = f"mode={data['execution_mode']} dry={data['margin_dry_run']} shorts={data['shorts_enabled']} {data['refreshed_at']}"
    add(stdscr, 0, max(2, width - len(right) - 2), right, curses.color_pair(1))


def candle_feed_text(data) -> str:
    feed = data.get("candle_feed") or {}
    if data.get("candle_error"):
        return "ERROR"
    latest = fr_datetime(feed.get("latest"), with_date=False) if feed.get("latest") else "-"
    return f"{feed.get('count', 0)} symbols @ {latest}"

def render_status(stdscr, y, x, h, w, data):
    box(stdscr, y, x, h, w, "Control")
    settings = data["settings"]
    total_pnl = data.get("pnl_summary", {}).get("total_pnl")
    momentum = data.get("momentum") or {}
    rows = [
        ("Execution", data["execution_mode"]),
        ("Order quote", settings.order_quote_amount),
        ("Quote assets", ",".join(settings.quote_assets)),
        ("Multiplier", data["margin_multiplier"]),
        ("Dry run", f"global={settings.dry_run} margin={data['margin_dry_run']}"),
        ("PNL total", f"{total_pnl:+.4f}" if total_pnl is not None else "-"),
        ("Candidates", f"{len(data.get('candidates') or [])}/{data.get('candidate_limit', '-')}") ,
        ("Candle feed", candle_feed_text(data)),
        ("Mom action", momentum.get("decision_action") or momentum.get("action", "-")),
        ("Mom result", momentum.get("execution_result", "-")),
        ("Refresh", data["refreshed_at"]),
    ]
    for i, (k, v) in enumerate(rows[: h - 2]):
        attr = curses.color_pair(2) if k == "PNL total" and _float(v, 0) >= 0 else curses.color_pair(5) if k == "PNL total" else curses.color_pair(3)
        add(stdscr, y + 1 + i, x + 2, f"{k:<12}", curses.color_pair(3))
        add(stdscr, y + 1 + i, x + 15, trunc(v, w - 18), attr)


def render_momentum(stdscr, y, x, h, w, data):
    box(stdscr, y, x, h, w, "Momentum Decision")
    m = data.get("momentum") or {}
    if not m:
        add(stdscr, y + 1, x + 2, "No momentum trade candidate yet", curses.color_pair(4))
        return
    rows = [
        ("Decision", m.get("decision_action") or m.get("action")),
        ("Symbol", m.get("symbol")),
        ("Target", m.get("target_symbol")),
        ("Status", m.get("status")),
        ("Reason", m.get("reason")),
        ("Order IDs", m.get("order_ids")),
        ("Fill IDs", m.get("fill_ids")),
        ("Best asset", m.get("best_asset.symbol")),
        ("Best price", m.get("best_asset.price")),
        ("Best score", m.get("best_asset.momentum_score")),
    ]
    for i, (k, v) in enumerate(rows[: h - 2]):
        color = curses.color_pair(2) if k == "Decision" and str(v).upper() in {"BUY", "HOLD"} else curses.color_pair(5) if k == "Decision" and str(v).upper() in {"SELL", "ROTATE"} else curses.color_pair(4)
        add(stdscr, y + 1 + i, x + 2, f"{k:<10}", curses.color_pair(3))
        add(stdscr, y + 1 + i, x + 14, trunc(v, w - 16), color)

def render_positions(stdscr, y, x, h, w, data):
    total_pnl = data.get("pnl_summary", {}).get("total_pnl")
    total_pnl_text = f"{total_pnl:+.4f}" if total_pnl is not None else "-"
    box(stdscr, y, x, h, w, f"Open Positions | PNL total {total_pnl_text}")
    rows = data["positions"][: max(0, h - 4)]
    add(stdscr, y + 1, x + 2, "Str Symbol     Side   Qty        Entry      Mark       PnL        TP/Target  Status    Ouvert", curses.A_BOLD)
    if not rows:
        add(stdscr, y + 2, x + 2, "No open positions", curses.color_pair(4))
        return
    for idx, (_, row) in enumerate(rows):
        symbol = row.get("execution_symbol") or row.get("signal_symbol")
        pnl = row.get("pnl")
        mark = row.get("mark_price")
        pnl_text = f"{pnl:+.4f}" if pnl is not None else "PNL?"
        mark_text = f"{mark:.8g}" if mark is not None else "-"
        result = safe(row.get("status") or position_result(row))
        opened = fr_datetime(row.get("opened_at") or row.get("created_at"), with_date=False)
        line = f"{safe(row.get('strategy_label')):<3} {safe(symbol):<10} {safe(row.get('side')):<6} {safe(row.get('quantity')):<10} {safe(row.get('entry_price')):<10} {mark_text:<10} {pnl_text:<10} {safe(row.get('target_price')):<10} {result:<9} {opened}"
        color = curses.color_pair(2) if target_reached(row) or (pnl is not None and pnl >= 0) else curses.color_pair(5) if pnl is not None else curses.color_pair(4)
        add(stdscr, y + 2 + idx, x + 2, trunc(line, w - 4), color)


def render_candidates(stdscr, y, x, h, w, data):
    box(stdscr, y, x, h, w, "Trade Candidates")
    candidates = data["candidates"]
    error = data["candidate_error"]
    if error:
        add(stdscr, y + 1, x + 2, trunc(f"API error: {error}", w - 4), curses.color_pair(5))
        return
    add(stdscr, y + 1, x + 2, f"received={len(candidates)} limit={data.get('candidate_limit', '-')} refresh={data['refreshed_at']}", curses.color_pair(3))
    if not candidates:
        add(stdscr, y + 2, x + 2, "No candidates returned", curses.color_pair(4))
        return
    add(stdscr, y + 2, x + 2, "Reçu          ID           Symbol   Stage    Side  Entry    TP       SL       Score  Status", curses.A_BOLD)
    for idx, row in enumerate(candidates[: max(0, h - 4)]):
        line = f"{candidate_received_at(row):<13} {safe(row.get('candidate_id')):<12} {safe(row.get('symbol')):<8} {safe(row.get('stage')):<8} {safe(row.get('side')):<5} {safe(row.get('entry_price')):<8} {safe(row.get('target_price')):<8} {safe(row.get('stop_loss') or row.get('stop_price')):<8} {safe(row.get('score')):<6} {safe(row.get('status'))}"
        add(stdscr, y + 3 + idx, x + 2, trunc(line, w - 4))


def _event_details(event: dict) -> str:
    payload = _payload(event)
    event_type = str(event.get("event_type") or "")
    if event_type.startswith("momentum_"):
        return "action={} sym={} buy={} sell={} result={} reason={}".format(payload.get("action"), payload.get("symbol"), payload.get("buy_symbol"), payload.get("sell_symbol"), payload.get("execution_result"), payload.get("reason") or payload.get("error"))
    parts = []
    for key, label in [("symbol", "sym"), ("execution_symbol", "sym"), ("signal_symbol", "sig"), ("side", "side"), ("mode", "mode"), ("reason", "reason"), ("error", "err"), ("quantity", "qty"), ("entry_price", "entry"), ("target_price", "tp")]:
        value = payload.get(key)
        if value is not None and value != "":
            parts.append(f"{label}={value}")
    if not parts:
        parts.append(json.dumps(payload, ensure_ascii=False, sort_keys=True)[:300])
    return " ".join(parts)


def render_events(stdscr, y, x, h, w, data):
    box(stdscr, y, x, h, w, "Important Errors / Last Activity")
    error_events = []
    normal_events = []
    for event in data["events"]:
        text = (str(event.get("event_type", "")) + " " + _event_details(event)).lower()
        if any(x in text for x in ["error", "failed", "blocked", "not_confirmed", "insufficient", "rejected", "mismatch"]):
            error_events.append(event)
        else:
            normal_events.append(event)
    events = (error_events[:3] + normal_events)[: max(0, h - 3)]
    if not events:
        add(stdscr, y + 1, x + 2, "No events", curses.color_pair(4))
        return
    add(stdscr, y + 1, x + 2, "Date FR        Candidate              Event / Details", curses.A_BOLD)
    for idx, event in enumerate(events):
        event_type = str(event.get("event_type", ""))
        details = _event_details(event)
        text = (event_type + " " + details).lower()
        level = curses.color_pair(5) if any(x in text for x in ["error", "failed", "blocked", "not_confirmed", "insufficient", "rejected", "mismatch"]) else curses.color_pair(2) if any(x in text for x in ["opened", "replayed", "attached", "filled", "bought", "sold"]) else curses.color_pair(4)
        timestamp = fr_datetime(event.get("timestamp"))
        candidate_id = trunc(event.get("candidate_id"), 21)
        line = f"{timestamp:<14} {candidate_id:<21} {event_type} | {details}"
        add(stdscr, y + 2 + idx, x + 2, trunc(line, w - 4), level)


def render_footer(stdscr, height, width, data):
    candle = candle_feed_text(data)
    if data.get("candle_error"):
        candle = "candle ERROR"
    text = f" q quit | r refresh | candle={candle} | candidates={data.get('candidate_limit', '-')} | auto {REFRESH_SECONDS}s | refresh={data['refreshed_at']}"
    add(stdscr, height - 1, 0, " " * (width - 1), curses.color_pair(1))
    add(stdscr, height - 1, 2, text, curses.color_pair(1))


def draw(stdscr, data):
    stdscr.erase()
    height, width = stdscr.getmaxyx()
    render_header(stdscr, width, data)
    if height < 18 or width < 80:
        add(stdscr, 2, 2, "Terminal too small. Use at least 80x18.", curses.color_pair(5))
        render_footer(stdscr, height, width, data)
        stdscr.refresh()
        return

    top_h = min(max(12, height // 3), 16)
    bottom_y = top_h + 3
    bottom_h = height - bottom_y - 1
    left_w = max(28, min(width // 3, 46))
    momentum_w = max(38, min(width // 3, 54))
    pos_x = left_w + 2
    pos_w = width - left_w - momentum_w - 5
    mom_x = pos_x + pos_w + 2
    candidates_w = max(46, int(width * 0.42))
    events_x = candidates_w + 2
    events_w = width - events_x - 1

    render_status(stdscr, 2, 1, top_h, left_w, data)
    render_positions(stdscr, 2, pos_x, top_h, max(36, pos_w), data)
    render_momentum(stdscr, 2, mom_x, top_h, momentum_w, data)
    render_candidates(stdscr, bottom_y, 1, bottom_h, candidates_w, data)
    render_events(stdscr, bottom_y, events_x, bottom_h, events_w, data)
    render_footer(stdscr, height, width, data)
    stdscr.refresh()


def main_loop(stdscr):
    curses.curs_set(0)
    stdscr.nodelay(True)
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_CYAN)
    curses.init_pair(2, curses.COLOR_GREEN, -1)
    curses.init_pair(3, curses.COLOR_CYAN, -1)
    curses.init_pair(4, curses.COLOR_WHITE, -1)
    curses.init_pair(5, curses.COLOR_RED, -1)
    data = snapshot()
    last_refresh = time.monotonic()
    while True:
        now = time.monotonic()
        ch = stdscr.getch()
        if ch in (ord("q"), ord("Q")):
            break
        if ch in (ord("r"), ord("R")) or now - last_refresh >= REFRESH_SECONDS:
            data = snapshot()
            last_refresh = now
        draw(stdscr, data)
        time.sleep(0.2)


def main():
    curses.wrapper(main_loop)


if __name__ == "__main__":
    main()

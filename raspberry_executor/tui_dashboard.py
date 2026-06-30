import curses
import json
import os
import time
from datetime import datetime

from raspberry_executor.kraken_client import KrakenClient
from raspberry_executor.config import load_settings
from raspberry_executor.margin_settings import execution_mode, margin_dry_run, margin_multiplier, shorts_enabled
from raspberry_executor.signalmaker_client import SignalMakerClient
from raspberry_executor.state import StateStore

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
    return fr_datetime(candidate.get("updated_at") or candidate.get("created_at") or candidate.get("timestamp") or candidate.get("exported_at"))


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
    return "buy" if 45 <= rsi <= 55 else "blocked"


def fetch_candidates(limit: int | None = None):
    try:
        settings = load_settings()
        client = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
        return client.get_open_candidates(limit=limit or candidate_display_limit()), None
    except Exception as exc:
        return [], str(exc)


def position_strategy(candidate_id: str, row: dict) -> str:
    if str(candidate_id).startswith("momentum-") or isinstance(row.get("momentum_decision"), dict) or str(row.get("strategy") or "").lower() == "momentum_rotation":
        return "mom"
    mode = str(row.get("mode") or "").lower()
    if "margin" in mode or mode in {"cross", "isolated"}:
        return "mgn"
    return "sig"


def enrich_positions_with_pnl(positions, settings):
    kraken = KrakenClient(settings.kraken_base_url, settings.kraken_api_key, settings.kraken_secret_key, dry_run=settings.dry_run or margin_dry_run())
    enriched = []
    total_pnl = 0.0
    total_ok = True
    price_cache = {}
    for candidate_id, row in positions:
        pos = dict(row)
        pos["strategy_label"] = position_strategy(candidate_id, pos)
        symbol = str(pos.get("execution_symbol") or pos.get("signal_symbol") or "").upper()
        side = str(pos.get("side") or "").lower()
        qty = _float(pos.get("quantity"))
        entry = _float(pos.get("entry_price"))
        try:
            if symbol not in price_cache:
                price_cache[symbol] = kraken.current_price(symbol)
            mark = price_cache[symbol]
            pnl = (mark - entry) * qty if side == "long" else (entry - mark) * qty
            pos["mark_price"] = mark
            pos["pnl"] = pnl
            total_pnl += pnl
        except Exception as exc:
            pos["mark_price"] = None
            pos["pnl"] = None
            pos["pnl_error"] = str(exc)
            total_ok = False
        enriched.append((candidate_id, pos))
    return enriched, {"total_pnl": total_pnl, "total_ok": total_ok}


def snapshot():
    state = StateStore()
    limit = candidate_display_limit()
    candidates, candidate_error = fetch_candidates(limit=limit)
    settings = load_settings()
    positions, pnl_summary = enrich_positions_with_pnl(list(state.open_positions().items()), settings)
    events = list(reversed(state.events()[-160:]))
    now = datetime.now().astimezone()
    return {
        "settings": settings,
        "execution_mode": execution_mode(),
        "margin_dry_run": margin_dry_run(),
        "margin_multiplier": margin_multiplier(),
        "shorts_enabled": shorts_enabled(),
        "positions": positions,
        "pnl_summary": pnl_summary,
        "events": events,
        "momentum": latest_momentum_decision(list(reversed(events))),
        "momentum_candidates": momentum_candidate_rows(candidates),
        "candidates": candidates,
        "candidate_error": candidate_error,
        "candidate_limit": limit,
        "refreshed_at": now.strftime("%d/%m %H:%M:%S"),
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
    add(stdscr, 0, 2, " SignalMaker Raspberry Executor TUI ", curses.color_pair(1) | curses.A_BOLD)
    right = f"mode={data['execution_mode']} dry={data['margin_dry_run']} shorts={data['shorts_enabled']} {data['refreshed_at']}"
    add(stdscr, 0, max(2, width - len(right) - 2), right, curses.color_pair(1))


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
        ("Mom cands", len(data.get("momentum_candidates") or [])),
        ("Mom action", momentum.get("action", "-")),
        ("Mom result", momentum.get("execution_result", "-")),
        ("Refresh", data["refreshed_at"]),
    ]
    for i, (k, v) in enumerate(rows[: h - 2]):
        attr = curses.color_pair(2) if k == "PNL total" and _float(v, 0) >= 0 else curses.color_pair(5) if k == "PNL total" else curses.color_pair(3)
        add(stdscr, y + 1 + i, x + 2, f"{k:<12}", curses.color_pair(3))
        add(stdscr, y + 1 + i, x + 15, trunc(v, w - 18), attr)


def render_momentum(stdscr, y, x, h, w, data):
    box(stdscr, y, x, h, w, "Momentum Candidates")
    rows = data.get("momentum_candidates") or []
    m = data.get("momentum") or {}
    if rows:
        add(stdscr, y + 1, x + 2, f"Trade candidates={len(rows)} last_action={safe(m.get('action'))}", curses.color_pair(3))
        add(stdscr, y + 2, x + 2, "Rank Symbol     RSI1H  Buy      Score      Status", curses.A_BOLD)
        for idx, row in enumerate(rows[: max(0, h - 4)]):
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            remote = payload.get("remote_candidate") if isinstance(payload.get("remote_candidate"), dict) else {}
            rank = remote.get("rank") or payload.get("rank") or "-"
            score = row.get("score") if row.get("score") is not None else remote.get("momentum_score")
            buyable = momentum_buyable_text(row)
            line = f"{safe(rank):<4} {safe(row.get('symbol')):<10} {safe(candidate_rsi_1h(row)):<6} {buyable:<8} {safe(score):<10} {safe(row.get('status'))}"
            add(stdscr, y + 3 + idx, x + 2, trunc(line, w - 4), curses.color_pair(2) if buyable == "buy" and str(row.get("status")) == "open" else curses.color_pair(4))
        return
    if not m:
        add(stdscr, y + 1, x + 2, "No momentum trade candidate yet", curses.color_pair(4))
        return
    decision = m.get("decision") if isinstance(m.get("decision"), dict) else {}
    target = decision.get("target_asset") if isinstance(decision.get("target_asset"), dict) else {}
    rows = [
        ("Action", m.get("action")),
        ("Symbol", m.get("symbol")),
        ("Buy", m.get("buy_symbol")),
        ("Sell", m.get("sell_symbol")),
        ("Trade", m.get("should_trade")),
        ("Result", m.get("execution_result")),
        ("Rank/Score", f"{safe(target.get('rank'))}/{safe(target.get('momentum_score'))}"),
        ("Next", m.get("next_check_at")),
        ("Reason", m.get("reason")),
    ]
    for i, (k, v) in enumerate(rows[: h - 2]):
        color = curses.color_pair(2) if k == "Action" and str(v).upper() in {"BUY", "HOLD"} else curses.color_pair(5) if k == "Action" and str(v).upper() in {"SELL", "ROTATE"} else curses.color_pair(4)
        add(stdscr, y + 1 + i, x + 2, f"{k:<10}", curses.color_pair(3))
        add(stdscr, y + 1 + i, x + 14, trunc(v, w - 16), color)

def render_positions(stdscr, y, x, h, w, data):
    total_pnl = data.get("pnl_summary", {}).get("total_pnl", 0.0)
    box(stdscr, y, x, h, w, f"Open Positions | PNL total {total_pnl:+.4f}")
    rows = data["positions"][: max(0, h - 4)]
    add(stdscr, y + 1, x + 2, "Str Symbol     Side   Qty        Entry      Mark       PNL        Target     TP        Result", curses.A_BOLD)
    if not rows:
        add(stdscr, y + 2, x + 2, "No open positions", curses.color_pair(4))
        return
    for idx, (_, row) in enumerate(rows):
        symbol = row.get("execution_symbol") or row.get("signal_symbol")
        pnl = row.get("pnl")
        mark = row.get("mark_price")
        pnl_text = f"{pnl:+.4f}" if pnl is not None else "PNL?"
        mark_text = f"{mark:.8g}" if mark is not None else "-"
        result = position_result(row)
        tp_text = safe(row.get('tp_order_id'), 'no-tp')
        line = f"{safe(row.get('strategy_label')):<3} {safe(symbol):<10} {safe(row.get('side')):<6} {safe(row.get('quantity')):<10} {safe(row.get('entry_price')):<10} {mark_text:<10} {pnl_text:<10} {safe(row.get('target_price')):<10} {tp_text:<9} {result}"
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
    add(stdscr, y + 2, x + 2, "Received       Symbol     Stage      Side   Status   Target", curses.A_BOLD)
    for idx, row in enumerate(candidates[: max(0, h - 4)]):
        line = f"{candidate_received_at(row):<14} {safe(row.get('symbol')):<10} {safe(row.get('stage')):<10} {safe(row.get('side')):<6} {safe(row.get('status')):<8} {safe(row.get('target_price'))}"
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
    box(stdscr, y, x, h, w, "Recent Events")
    events = data["events"][: max(0, h - 3)]
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
    text = f" q quit | r refresh | candidates={data.get('candidate_limit', '-')} | auto {REFRESH_SECONDS}s | refresh={data['refreshed_at']}"
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

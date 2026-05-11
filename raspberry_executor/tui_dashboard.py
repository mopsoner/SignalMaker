import curses
import time
from datetime import datetime

from raspberry_executor.config import load_settings
from raspberry_executor.margin_settings import execution_mode, margin_dry_run, margin_multiplier, shorts_enabled
from raspberry_executor.signalmaker_client import SignalMakerClient
from raspberry_executor.state import StateStore

REFRESH_SECONDS = 5


def safe(value, default="-"):
    if value is None:
        return default
    text = str(value)
    return text if text else default


def trunc(value, width):
    text = safe(value, "")
    return text if len(text) <= width else text[: max(0, width - 1)] + "…"


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


def fetch_candidates(limit=8):
    try:
        settings = load_settings()
        client = SignalMakerClient(settings.signalmaker_base_url, settings.gateway_id)
        return client.get_open_candidates(limit=limit), None
    except Exception as exc:
        return [], str(exc)


def snapshot():
    state = StateStore()
    candidates, candidate_error = fetch_candidates()
    settings = load_settings()
    return {
        "settings": settings,
        "execution_mode": execution_mode(),
        "margin_dry_run": margin_dry_run(),
        "margin_multiplier": margin_multiplier(),
        "shorts_enabled": shorts_enabled(),
        "positions": list(state.open_positions().items()),
        "events": list(reversed(state.events()[-120:])),
        "candidates": candidates,
        "candidate_error": candidate_error,
        "refreshed_at": datetime.now().strftime("%H:%M:%S"),
    }


def render_header(stdscr, width, data):
    add(stdscr, 0, 0, " " * (width - 1), curses.color_pair(1))
    add(stdscr, 0, 2, " SignalMaker Raspberry TUI ", curses.color_pair(1) | curses.A_BOLD)
    right = f"mode={data['execution_mode']} margin_dry={data['margin_dry_run']} shorts={data['shorts_enabled']} data={data['refreshed_at']}"
    add(stdscr, 0, max(2, width - len(right) - 2), right, curses.color_pair(1))


def render_status(stdscr, y, x, h, w, data):
    box(stdscr, y, x, h, w, "Control")
    settings = data["settings"]
    rows = [
        ("Execution", data["execution_mode"]),
        ("Order quote", settings.order_quote_amount),
        ("Quote assets", ",".join(settings.quote_assets)),
        ("Multiplier", data["margin_multiplier"]),
        ("Dry run", f"global={settings.dry_run} margin={data['margin_dry_run']}"),
        ("Shorts", data["shorts_enabled"]),
        ("Data refresh", data["refreshed_at"]),
    ]
    for i, (k, v) in enumerate(rows[: h - 2]):
        add(stdscr, y + 1 + i, x + 2, f"{k:<12}", curses.color_pair(3))
        add(stdscr, y + 1 + i, x + 15, trunc(v, w - 18))


def render_positions(stdscr, y, x, h, w, data):
    box(stdscr, y, x, h, w, "Open Positions")
    rows = data["positions"][: max(0, h - 4)]
    add(stdscr, y + 1, x + 2, "Symbol     Side   Mode       Qty        Entry      TP/SL", curses.A_BOLD)
    if not rows:
        add(stdscr, y + 2, x + 2, "No open positions", curses.color_pair(4))
        return
    for idx, (_, row) in enumerate(rows):
        symbol = row.get("execution_symbol") or row.get("signal_symbol")
        line = f"{safe(symbol):<10} {safe(row.get('side')):<6} {safe(row.get('mode')):<10} {safe(row.get('quantity')):<10} {safe(row.get('entry_price')):<10} {safe(row.get('target_price'))}/{safe(row.get('stop_price'))}"
        color = curses.color_pair(2) if str(row.get("side")).lower() == "long" else curses.color_pair(5)
        add(stdscr, y + 2 + idx, x + 2, trunc(line, w - 4), color)


def render_candidates(stdscr, y, x, h, w, data):
    box(stdscr, y, x, h, w, "SignalMaker Candidates")
    candidates = data["candidates"]
    error = data["candidate_error"]
    if error:
        add(stdscr, y + 1, x + 2, trunc(f"API error: {error}", w - 4), curses.color_pair(5))
        return
    add(stdscr, y + 1, x + 2, f"received={len(candidates)} refreshed={data['refreshed_at']}", curses.color_pair(3))
    if not candidates:
        add(stdscr, y + 2, x + 2, "No candidates returned", curses.color_pair(4))
        return
    add(stdscr, y + 2, x + 2, "Symbol     Side   Status    Stop        Target", curses.A_BOLD)
    for idx, row in enumerate(candidates[: max(0, h - 4)]):
        line = f"{safe(row.get('symbol')):<10} {safe(row.get('side')):<6} {safe(row.get('status')):<9} {safe(row.get('stop_price')):<11} {safe(row.get('target_price'))}"
        add(stdscr, y + 3 + idx, x + 2, trunc(line, w - 4))


def render_events(stdscr, y, x, h, w, data):
    box(stdscr, y, x, h, w, "Recent Events")
    events = data["events"][: max(0, h - 2)]
    if not events:
        add(stdscr, y + 1, x + 2, "No events", curses.color_pair(4))
        return
    for idx, event in enumerate(events):
        event_type = str(event.get("event_type", ""))
        level = curses.color_pair(5) if "error" in event_type or "failed" in event_type or "not_confirmed" in event_type else curses.color_pair(4)
        line = f"{safe(event.get('timestamp'))[-8:]} {safe(event.get('candidate_id'))} {event_type}"
        add(stdscr, y + 1 + idx, x + 2, trunc(line, w - 4), level)


def render_footer(stdscr, height, width, data):
    text = f" q quit | r force refresh | auto-refresh {REFRESH_SECONDS}s | logs hidden | data={data['refreshed_at']} | clock="
    add(stdscr, height - 1, 0, " " * (width - 1), curses.color_pair(1))
    add(stdscr, height - 1, 2, text + datetime.now().strftime("%H:%M:%S"), curses.color_pair(1))


def draw(stdscr, data):
    stdscr.erase()
    height, width = stdscr.getmaxyx()
    render_header(stdscr, width, data)
    if height < 18 or width < 80:
        add(stdscr, 2, 2, "Terminal too small. Use at least 80x18.", curses.color_pair(5))
        render_footer(stdscr, height, width, data)
        stdscr.refresh()
        return
    top_h = 9
    remaining_h = height - top_h - 4
    left_w = max(30, width // 3)
    render_status(stdscr, 2, 1, top_h, left_w, data)
    render_positions(stdscr, 2, left_w + 2, top_h, width - left_w - 3, data)
    render_candidates(stdscr, top_h + 3, 1, remaining_h, width // 2 - 2, data)
    render_events(stdscr, top_h + 3, width // 2, remaining_h, width // 2 - 1, data)
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

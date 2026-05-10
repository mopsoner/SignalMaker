import curses
import time
from datetime import datetime

from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import tail_logs
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
    if len(text) <= width:
        return text
    return text[: max(0, width - 1)] + "…"


def add(stdscr, y, x, text, attr=0):
    height, width = stdscr.getmaxyx()
    if y < 0 or y >= height or x >= width:
        return
    try:
        stdscr.addstr(y, x, str(text)[: max(0, width - x - 1)], attr)
    except curses.error:
        pass


def box(stdscr, y, x, h, w, title=""):
    height, width = stdscr.getmaxyx()
    if y >= height or x >= width or h < 3 or w < 8:
        return
    h = min(h, height - y)
    w = min(w, width - x)
    try:
        stdscr.attron(curses.color_pair(4))
        stdscr.border(0, 0, 0, 0)
        stdscr.attroff(curses.color_pair(4))
    except Exception:
        pass
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


def render_header(stdscr, width):
    mode = execution_mode()
    dry = margin_dry_run()
    shorts = shorts_enabled()
    title = " SignalMaker Raspberry TUI "
    add(stdscr, 0, 0, " " * (width - 1), curses.color_pair(1))
    add(stdscr, 0, 2, title, curses.color_pair(1) | curses.A_BOLD)
    add(stdscr, 0, max(2, width - 52), f"mode={mode} margin_dry={dry} shorts={shorts}", curses.color_pair(1))


def render_status(stdscr, y, x, h, w):
    box(stdscr, y, x, h, w, "Control")
    settings = load_settings()
    rows = [
        ("Execution", execution_mode()),
        ("Order quote", settings.order_quote_amount),
        ("Quote assets", ",".join(settings.quote_assets)),
        ("Multiplier", margin_multiplier()),
        ("Dry run", f"global={settings.dry_run} margin={margin_dry_run()}"),
        ("Shorts", shorts_enabled()),
        ("Poll", f"{settings.poll_seconds}s"),
    ]
    for i, (k, v) in enumerate(rows[: h - 2]):
        add(stdscr, y + 1 + i, x + 2, f"{k:<12}", curses.color_pair(3))
        add(stdscr, y + 1 + i, x + 15, trunc(v, w - 18))


def render_positions(stdscr, y, x, h, w):
    box(stdscr, y, x, h, w, "Open Positions")
    rows = list(StateStore().open_positions().items())[: max(0, h - 4)]
    add(stdscr, y + 1, x + 2, "Symbol     Side   Mode       Qty        Entry      TP/SL", curses.A_BOLD)
    if not rows:
        add(stdscr, y + 2, x + 2, "No open positions", curses.color_pair(4))
        return
    for idx, (_, row) in enumerate(rows):
        symbol = row.get("execution_symbol") or row.get("signal_symbol")
        line = f"{safe(symbol):<10} {safe(row.get('side')):<6} {safe(row.get('mode')):<10} {safe(row.get('quantity')):<10} {safe(row.get('entry_price')):<10} {safe(row.get('target_price'))}/{safe(row.get('stop_price'))}"
        color = curses.color_pair(2) if str(row.get("side")).lower() == "long" else curses.color_pair(5)
        add(stdscr, y + 2 + idx, x + 2, trunc(line, w - 4), color)


def render_candidates(stdscr, y, x, h, w, candidates, error):
    box(stdscr, y, x, h, w, "SignalMaker Candidates")
    if error:
        add(stdscr, y + 1, x + 2, trunc(f"API error: {error}", w - 4), curses.color_pair(5))
        return
    add(stdscr, y + 1, x + 2, f"received={len(candidates)}", curses.color_pair(3))
    if not candidates:
        add(stdscr, y + 2, x + 2, "No candidates returned", curses.color_pair(4))
        return
    add(stdscr, y + 2, x + 2, "Symbol     Side   Status    Stop        Target", curses.A_BOLD)
    for idx, row in enumerate(candidates[: max(0, h - 4)]):
        line = f"{safe(row.get('symbol')):<10} {safe(row.get('side')):<6} {safe(row.get('status')):<9} {safe(row.get('stop_price')):<11} {safe(row.get('target_price'))}"
        add(stdscr, y + 3 + idx, x + 2, trunc(line, w - 4))


def render_events(stdscr, y, x, h, w):
    box(stdscr, y, x, h, w, "Recent Events")
    events = list(reversed(StateStore().events()[-max(1, h - 2):]))
    if not events:
        add(stdscr, y + 1, x + 2, "No events", curses.color_pair(4))
        return
    for idx, event in enumerate(events[: h - 2]):
        level = curses.color_pair(5) if "error" in str(event.get("event_type", "")) or "failed" in str(event.get("event_type", "")) else curses.color_pair(4)
        line = f"{safe(event.get('timestamp'))[-8:]} {safe(event.get('candidate_id'))} {safe(event.get('event_type'))}"
        add(stdscr, y + 1 + idx, x + 2, trunc(line, w - 4), level)


def render_logs(stdscr, y, x, h, w):
    box(stdscr, y, x, h, w, "Logs")
    logs = tail_logs(max(1, h - 2))
    for idx, line in enumerate(logs[-(h - 2):]):
        attr = curses.color_pair(5) if "ERROR" in line or "failed" in line.lower() else curses.color_pair(4)
        add(stdscr, y + 1 + idx, x + 2, trunc(line, w - 4), attr)


def render_footer(stdscr, height, width):
    text = " q quit | r refresh | auto-refresh 5s | no web/browser "
    add(stdscr, height - 1, 0, " " * (width - 1), curses.color_pair(1))
    add(stdscr, height - 1, 2, text + datetime.now().strftime("%H:%M:%S"), curses.color_pair(1))


def draw(stdscr, candidates, candidate_error):
    stdscr.erase()
    height, width = stdscr.getmaxyx()
    render_header(stdscr, width)
    if height < 22 or width < 80:
        add(stdscr, 2, 2, "Terminal too small. Use at least 80x22.", curses.color_pair(5))
        render_footer(stdscr, height, width)
        stdscr.refresh()
        return
    top_h = 9
    mid_h = max(7, (height - top_h - 3) // 2)
    left_w = max(30, width // 3)
    render_status(stdscr, 2, 1, top_h, left_w)
    render_positions(stdscr, 2, left_w + 2, top_h, width - left_w - 3)
    render_candidates(stdscr, top_h + 3, 1, mid_h, width // 2 - 2, candidates, candidate_error)
    render_events(stdscr, top_h + 3, width // 2, mid_h, width // 2 - 1)
    render_logs(stdscr, top_h + mid_h + 4, 1, height - (top_h + mid_h + 5), width - 2)
    render_footer(stdscr, height, width)
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
    candidates, error = fetch_candidates()
    last_refresh = 0.0
    while True:
        now = time.monotonic()
        ch = stdscr.getch()
        if ch in (ord("q"), ord("Q")):
            break
        if ch in (ord("r"), ord("R")) or now - last_refresh >= REFRESH_SECONDS:
            candidates, error = fetch_candidates()
            last_refresh = now
        draw(stdscr, candidates, error)
        time.sleep(0.2)


def main():
    curses.wrapper(main_loop)


if __name__ == "__main__":
    main()

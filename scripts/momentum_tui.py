#!/usr/bin/env python3
from __future__ import annotations

import curses
import json
import os
import time
from datetime import datetime
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

API_BASE = os.getenv("SIGNALMAKER_API_BASE", "http://127.0.0.1:5000")
REFRESH_SECONDS = int(os.getenv("MOMENTUM_TUI_REFRESH", "30"))
LIMIT = int(os.getenv("MOMENTUM_TUI_LIMIT", "100"))


def fetch_json(path: str):
    req = Request(f"{API_BASE}{path}", headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=8) as response:
        return json.loads(response.read().decode("utf-8"))


def fmt(value, digits=2, width=8):
    if value is None:
        return "—".rjust(width)
    try:
        return f"{float(value):.{digits}f}".rjust(width)
    except Exception:
        return str(value)[:width].rjust(width)


def fmt_price(value):
    if value is None:
        return "—".rjust(12)
    try:
        price = float(value)
        if price >= 100:
            return f"{price:.2f}".rjust(12)
        if price >= 1:
            return f"{price:.4f}".rjust(12)
        return f"{price:.6f}".rjust(12)
    except Exception:
        return str(value)[:12].rjust(12)


def safe_add(stdscr, y, x, text, attr=0):
    height, width = stdscr.getmaxyx()
    if y < 0 or y >= height or x >= width:
        return
    try:
        stdscr.addstr(y, x, text[: max(0, width - x - 1)], attr)
    except curses.error:
        pass


def color_for(row):
    cls = row.get("classification")
    score = float(row.get("momentum_score") or 0)
    if cls in ("strong_bull", "bull") or score >= 10:
        return curses.color_pair(2)
    if cls == "neutral_bull" or score >= 0:
        return curses.color_pair(3)
    if cls == "neutral_bear":
        return curses.color_pair(4)
    return curses.color_pair(5)


def class_label(value):
    return {
        "strong_bull": "STRONG",
        "bull": "BULL",
        "neutral_bull": "N-BULL",
        "neutral_bear": "N-BEAR",
        "bear": "BEAR",
    }.get(value, value or "—")


def draw_header(stdscr, rows, last_update, error, filter_name):
    safe_add(stdscr, 0, 0, "SignalMaker Momentum TUI", curses.A_BOLD | curses.color_pair(1))
    safe_add(stdscr, 0, 28, f"API {API_BASE}", curses.color_pair(6))
    safe_add(stdscr, 1, 0, f"Rows: {len(rows)}  Refresh: {REFRESH_SECONDS}s  Filter: {filter_name}  Updated: {last_update or '—'}", curses.color_pair(6))
    safe_add(stdscr, 2, 0, "Keys: q quit | r refresh | ↑/↓ scroll | a all | s strong/bull | n neutral | b bear", curses.color_pair(6))
    if error:
        safe_add(stdscr, 3, 0, f"ERROR: {error}", curses.color_pair(5) | curses.A_BOLD)


def draw_top10(stdscr, rows):
    safe_add(stdscr, 5, 0, "TOP 10 MOMENTUM FORT", curses.A_BOLD | curses.color_pair(1))
    y = 6
    for row in rows[:10]:
        text = f"#{row.get('rank', '—'):>2} {row.get('symbol', '—'):<12} score {fmt(row.get('momentum_score'), 2, 7)}  15m {fmt(row.get('momentum_15m'), 2, 7)}  1h {fmt(row.get('momentum_1h'), 2, 7)}  4h {fmt(row.get('momentum_4h'), 2, 7)}  {class_label(row.get('classification'))}"
        safe_add(stdscr, y, 0, text, color_for(row))
        y += 1


def draw_table(stdscr, rows, offset):
    height, width = stdscr.getmaxyx()
    start_y = 18
    safe_add(stdscr, start_y - 2, 0, "MOMENTUM SCANNER", curses.A_BOLD | curses.color_pair(1))
    header = "Rank Symbol        Price        Score     15m      1h      4h      RSI15   RSI1h   RSI4h   Class   Data"
    safe_add(stdscr, start_y - 1, 0, header, curses.A_BOLD | curses.color_pair(6))
    available = max(0, height - start_y - 1)
    visible = rows[offset: offset + available]
    for idx, row in enumerate(visible):
        y = start_y + idx
        line = (
            f"{str(row.get('rank', '—')).rjust(4)} "
            f"{str(row.get('symbol', '—'))[:12].ljust(12)} "
            f"{fmt_price(row.get('price'))} "
            f"{fmt(row.get('momentum_score'), 2, 8)} "
            f"{fmt(row.get('momentum_15m'), 2, 7)} "
            f"{fmt(row.get('momentum_1h'), 2, 7)} "
            f"{fmt(row.get('momentum_4h'), 2, 7)} "
            f"{fmt(row.get('rsi_15m'), 1, 7)} "
            f"{fmt(row.get('rsi_1h'), 1, 7)} "
            f"{fmt(row.get('rsi_4h'), 1, 7)} "
            f"{class_label(row.get('classification'))[:7].ljust(7)} "
            f"{str(row.get('data_quality', '—'))[:12]}"
        )
        safe_add(stdscr, y, 0, line, color_for(row))
    safe_add(stdscr, height - 1, 0, f"Scroll {offset + 1 if rows else 0}-{min(offset + available, len(rows))}/{len(rows)}", curses.color_pair(6))


def apply_filter(rows, filter_name):
    if filter_name == "strong":
        return [r for r in rows if r.get("classification") in ("strong_bull", "bull")]
    if filter_name == "neutral":
        return [r for r in rows if r.get("classification") in ("neutral_bull", "neutral_bear")]
    if filter_name == "bear":
        return [r for r in rows if r.get("classification") == "bear"]
    return rows


def setup_colors():
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_CYAN, -1)
    curses.init_pair(2, curses.COLOR_GREEN, -1)
    curses.init_pair(3, curses.COLOR_YELLOW, -1)
    curses.init_pair(4, curses.COLOR_MAGENTA, -1)
    curses.init_pair(5, curses.COLOR_RED, -1)
    curses.init_pair(6, curses.COLOR_WHITE, -1)


def main(stdscr):
    curses.curs_set(0)
    stdscr.nodelay(True)
    stdscr.keypad(True)
    setup_colors()

    rows = []
    filtered = []
    error = None
    last_update = None
    last_fetch = 0
    offset = 0
    filter_name = "all"

    while True:
        now = time.time()
        if now - last_fetch >= REFRESH_SECONDS or not rows:
            try:
                rows = fetch_json(f"/api/v1/momentum/ranking?limit={LIMIT}")
                error = None
                last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                last_fetch = now
                filtered = apply_filter(rows, filter_name)
                offset = min(offset, max(0, len(filtered) - 1))
            except (URLError, HTTPError, TimeoutError, OSError, json.JSONDecodeError) as exc:
                error = str(exc)
                last_fetch = now

        stdscr.erase()
        draw_header(stdscr, filtered, last_update, error, filter_name)
        draw_top10(stdscr, rows)
        draw_table(stdscr, filtered, offset)
        stdscr.refresh()

        key = stdscr.getch()
        if key in (ord("q"), ord("Q")):
            break
        if key in (ord("r"), ord("R")):
            last_fetch = 0
        if key == curses.KEY_DOWN:
            offset = min(offset + 1, max(0, len(filtered) - 1))
        if key == curses.KEY_UP:
            offset = max(0, offset - 1)
        if key in (ord("a"), ord("A")):
            filter_name = "all"
            filtered = apply_filter(rows, filter_name)
            offset = 0
        if key in (ord("s"), ord("S")):
            filter_name = "strong"
            filtered = apply_filter(rows, filter_name)
            offset = 0
        if key in (ord("n"), ord("N")):
            filter_name = "neutral"
            filtered = apply_filter(rows, filter_name)
            offset = 0
        if key in (ord("b"), ord("B")):
            filter_name = "bear"
            filtered = apply_filter(rows, filter_name)
            offset = 0

        time.sleep(0.1)


if __name__ == "__main__":
    curses.wrapper(main)

"""Read-only Raspberry terminal UI aligned with the local FastAPI website API."""
from __future__ import annotations

import curses
import os
import time
from collections.abc import Mapping
from typing import Any

from raspberry_executor.tui_api import BASE_URL, api_get, api_request, as_rows

REFRESH_SECONDS = int(os.getenv("SIGNALMAKER_TUI_REFRESH", "10") or "10")
SECRET_KEYS = {"kraken_api_key", "kraken_secret_key", "telegram_secret", "discord_url"}
SECTIONS = ["general", "executor", "kraken", "market_data", "strategy", "live", "bot", "momentum", "notifications"]

MENU = [
    ("Status / Health", "status"), ("Services / Workers", "services"), ("Assets", "assets"),
    ("Momentum", "momentum"), ("Trade Candidates", "candidates"), ("Positions", "positions"),
    ("Orders / Fills", "orders"), ("Market Data", "market"), ("Admin Settings", "settings"),
    ("Logs", "logs"), ("Kraken Diagnostics", "kraken"), ("Candle Feed Status", "candle"), ("Quit", "quit"),
]



def mask(key: str, value: Any) -> str:
    if key in SECRET_KEYS:
        text = "" if value is None else str(value)
        return f"SET length={len(text)}" if text else "NOT SET"
    return "-" if value is None else str(value)


def flat(row: Any, columns: list[str] | None = None) -> list[str]:
    if not isinstance(row, Mapping):
        return [str(row)]
    keys = columns or list(row.keys())[:8]
    return [mask(k, row.get(k)) for k in keys]


def add(stdscr, y: int, x: int, text: Any, attr: int = 0) -> None:
    h, w = stdscr.getmaxyx()
    if y < 0 or y >= h or x >= w:
        return
    try:
        stdscr.addstr(y, x, str(text)[: max(0, w - x - 1)], attr)
    except curses.error:
        pass


def draw_table(stdscr, y: int, rows: list[Any], columns: list[str] | None = None, max_rows: int | None = None) -> int:
    max_y, max_x = stdscr.getmaxyx()
    if not rows:
        add(stdscr, y, 2, "No data returned.", curses.A_DIM); return y + 1
    columns = columns or (list(rows[0].keys())[:6] if isinstance(rows[0], Mapping) else ["value"])
    widths = [max(8, min(24, (max_x - 4) // max(1, len(columns)))) for _ in columns]
    add(stdscr, y, 2, " ".join(c[:widths[i]].ljust(widths[i]) for i, c in enumerate(columns)), curses.A_BOLD); y += 1
    for row in rows[: max_rows or max(1, max_y - y - 2)]:
        vals = flat(row, columns)
        add(stdscr, y, 2, " ".join(vals[i][:widths[i]].ljust(widths[i]) for i in range(len(columns))))
        y += 1
        if y >= max_y - 2: break
    return y


def fetch_screen(kind: str) -> tuple[str, list[Any], list[str] | None, list[str]]:
    notes: list[str] = []
    if kind == "status":
        return "Status / Health", [api_get("/healthz"), api_get("/api/v1/health")], None, [f"Base URL: {BASE_URL}"]
    if kind == "services":
        rows = as_rows(api_get("/api/v1/services")) + as_rows(api_get("/api/v1/admin/workers"))
        return "Services / Workers", rows, None, []
    mapping = {
        "assets": ("Assets", "/api/v1/assets", {"limit": 100, "sort_by": "updated_at"}),
        "momentum": ("Momentum", "/api/v1/momentum", {"limit": 100}),
        "candidates": ("Trade Candidates", "/api/v1/trade-candidates", {"limit": 100}),
        "positions": ("Positions", "/api/v1/positions", {"limit": 100}),
        "market": ("Market Data", "/api/v1/market-data/candles", {"limit": 100, "latest": "true"}),
    }
    if kind in mapping:
        title, path, params = mapping[kind]; return title, as_rows(api_get(path, params)), None, []
    if kind == "orders":
        return "Orders / Fills", as_rows(api_get("/api/v1/orders", {"limit": 50})) + as_rows(api_get("/api/v1/fills", {"limit": 50})), None, []
    if kind == "settings":
        payload = api_get("/api/v1/admin/settings"); rows=[]
        for sec in SECTIONS:
            data = payload.get(sec, {}) if isinstance(payload, dict) else {}
            rows.append({"section": sec, "key": "", "value": ""})
            rows += [{"section": "", "key": k, "value": mask(k, v)} for k, v in sorted(data.items())]
        return "Admin Settings (read-only, secrets masked)", rows, ["section", "key", "value"], []
    if kind == "logs":
        lines=[]
        for worker in ("executor", "pipeline", "scheduler"):
            p=api_get(f"/api/v1/admin/logs/{worker}", {"lines": 30}); lines.append({"worker": worker, "line": "---"})
            lines += [{"worker": worker, "line": line} for line in p.get("lines", [])[-30:]] if isinstance(p, dict) else []
        return "Logs", lines, ["worker", "line"], []
    if kind == "kraken":
        settings = api_get("/api/v1/admin/settings"); kr = settings.get("kraken", {}) if isinstance(settings, dict) else {}; ex = settings.get("executor", {}) if isinstance(settings, dict) else {}
        rows = [{"key": "exchange", "value": ex.get("execution_exchange")}, {"key": "execution_mode", "value": ex.get("execution_mode", "cross")}, {"key": "quote_assets", "value": ex.get("quote_assets") or settings.get("market_data", {}).get("kraken_quote_assets", "-") if isinstance(settings, dict) else "-"}, {"key": "kraken_rest_base", "value": kr.get("kraken_rest_base") or kr.get("kraken_base_url")}, {"key": "kraken_api_key", "value": mask("kraken_api_key", kr.get("kraken_api_key"))}, {"key": "kraken_secret_key", "value": mask("kraken_secret_key", kr.get("kraken_secret_key"))}]
        try: rows += [{"key": f"test_{k}", "value": v} for k, v in api_request("/api/v1/admin/test/kraken", method="POST").items()]
        except Exception as exc: notes.append(str(exc))
        return "Kraken Diagnostics (dry-run/validate only)", rows, ["key", "value"], notes
    if kind == "candle":
        settings = api_get("/api/v1/admin/settings"); md = settings.get("market_data", {}) if isinstance(settings, dict) else {}
        summary = as_rows(api_get("/api/v1/market-data/candles/summary"))
        notes = ["CANDLE_FEED_MAX_SYMBOLS=0 means all symbols", f"Current max symbols: {md.get('candle_feed_max_symbols', os.getenv('CANDLE_FEED_MAX_SYMBOLS', '-'))}", f"Resolved symbol count: {len(summary)}"]
        return "Candle Feed Status", summary, None, notes
    return "Unknown", [], None, []


def main(stdscr) -> None:
    curses.curs_set(0); stdscr.nodelay(True); selected = 0; current = "status"; last = 0; cached = None
    while True:
        ch = stdscr.getch()
        if ch in (ord('q'), 27): break
        if ch in (curses.KEY_UP, ord('k')): selected = (selected - 1) % len(MENU)
        if ch in (curses.KEY_DOWN, ord('j')): selected = (selected + 1) % len(MENU)
        if ch in (10, 13):
            if MENU[selected][1] == "quit": break
            current = MENU[selected][1]; last = 0
        if time.time() - last >= REFRESH_SECONDS:
            try: cached = (*fetch_screen(current), None)
            except Exception as exc: cached = (MENU[selected][0], [], None, [], str(exc))
            last = time.time()
        stdscr.erase(); h,w = stdscr.getmaxyx(); add(stdscr,0,0," SignalMaker Raspberry Executor TUI ".ljust(w-1), curses.A_REVERSE|curses.A_BOLD)
        for i,(label,kind) in enumerate(MENU): add(stdscr,i+2,2,("> " if i==selected else "  ")+label, curses.A_REVERSE if i==selected else 0)
        title, rows, cols, notes, err = cached or ("Loading", [], None, [], None)
        add(stdscr,1,28,title,curses.A_BOLD); y=3
        for note in notes: add(stdscr,y,28,note,curses.A_DIM); y+=1
        if err: add(stdscr,y,28,"ERROR: "+err,curses.A_BOLD); y+=1
        draw_table(stdscr,y,rows,cols)
        add(stdscr,h-1,0,f" ↑/↓ select Enter open q quit | refresh {REFRESH_SECONDS}s | {BASE_URL}"[:w-1], curses.A_REVERSE)
        stdscr.refresh(); time.sleep(0.1)


def expert_main() -> None:
    curses.wrapper(main)


def overview_main() -> None:
    from raspberry_executor.tui_dashboard import main as dashboard_main

    dashboard_main()


def cli() -> None:
    import sys

    mode = os.getenv("SIGNALMAKER_TUI_MODE", "overview").strip().lower()
    if "--expert" in sys.argv or mode in {"expert", "full", "legacy"}:
        expert_main()
        return
    overview_main()


if __name__ == "__main__":
    cli()

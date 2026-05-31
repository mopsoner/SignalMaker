import curses
import os
import time
import traceback
from datetime import datetime

from raspberry_executor import tui_dashboard as dashboard

REFRESH_SECONDS = dashboard.REFRESH_SECONDS


def _add(stdscr, y: int, x: int, text: str, attr: int = 0) -> None:
    height, width = stdscr.getmaxyx()
    if y < 0 or y >= height or x < 0 or x >= width:
        return
    try:
        stdscr.addstr(y, x, str(text)[: max(0, width - x - 1)], attr)
    except curses.error:
        pass


def _init_screen(stdscr) -> None:
    try:
        curses.curs_set(0)
    except curses.error:
        pass
    stdscr.nodelay(True)
    curses.start_color()
    try:
        curses.use_default_colors()
    except curses.error:
        pass
    curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLUE)
    curses.init_pair(2, curses.COLOR_GREEN, -1)
    curses.init_pair(3, curses.COLOR_CYAN, -1)
    curses.init_pair(4, curses.COLOR_WHITE, -1)
    curses.init_pair(5, curses.COLOR_RED, -1)


def _draw_boot(stdscr, message: str = "Starting") -> None:
    stdscr.erase()
    height, width = stdscr.getmaxyx()
    title = "SignalMaker Raspberry TUI"
    _add(stdscr, 0, 0, " " * max(0, width - 1), curses.color_pair(1) | curses.A_BOLD)
    _add(stdscr, 0, 2, f" {title} ", curses.color_pair(1) | curses.A_BOLD)
    _add(stdscr, 2, 2, message, curses.color_pair(3) | curses.A_BOLD)
    _add(stdscr, 4, 2, f"TERM={os.getenv('TERM', '')} | {datetime.now().strftime('%d/%m %H:%M:%S')}", curses.color_pair(4))
    _add(stdscr, max(0, height - 1), 2, "q quit | r retry | auto refresh", curses.color_pair(1))
    stdscr.refresh()


def _draw_error(stdscr, exc: BaseException) -> None:
    stdscr.erase()
    height, width = stdscr.getmaxyx()
    _add(stdscr, 0, 0, " " * max(0, width - 1), curses.color_pair(1) | curses.A_BOLD)
    _add(stdscr, 0, 2, " SignalMaker Raspberry TUI - ERROR ", curses.color_pair(1) | curses.A_BOLD)
    _add(stdscr, 2, 2, "Le TUI est lancé, mais le chargement des données a échoué.", curses.color_pair(5) | curses.A_BOLD)
    _add(stdscr, 4, 2, f"Erreur: {type(exc).__name__}: {exc}", curses.color_pair(5))
    tb = traceback.format_exc().splitlines()[-8:]
    for idx, line in enumerate(tb):
        if 6 + idx >= height - 2:
            break
        _add(stdscr, 6 + idx, 2, line, curses.color_pair(4))
    _add(stdscr, max(0, height - 1), 2, "r retry | q quit | voir: journalctl -u signalmaker-tui.service -n 120 --no-pager", curses.color_pair(1))
    stdscr.refresh()


def _snapshot_with_screen(stdscr):
    _draw_boot(stdscr, "Loading settings, local state, candidates and positions...")
    return dashboard.snapshot()


def main_loop(stdscr) -> None:
    _init_screen(stdscr)
    data = None
    last_refresh = 0.0
    last_error: BaseException | None = None

    while True:
        now = time.monotonic()
        ch = stdscr.getch()
        if ch in (ord("q"), ord("Q")):
            break

        should_refresh = data is None or ch in (ord("r"), ord("R")) or now - last_refresh >= REFRESH_SECONDS
        if should_refresh:
            try:
                data = _snapshot_with_screen(stdscr)
                last_error = None
                last_refresh = time.monotonic()
            except BaseException as exc:
                last_error = exc
                data = None
                last_refresh = time.monotonic()

        if data is not None:
            dashboard.draw(stdscr, data)
        elif last_error is not None:
            _draw_error(stdscr, last_error)
        else:
            _draw_boot(stdscr)

        time.sleep(0.2)


def main() -> None:
    curses.wrapper(main_loop)


if __name__ == "__main__":
    main()

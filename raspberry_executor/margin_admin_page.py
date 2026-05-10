from html import escape
from urllib.parse import parse_qs

from raspberry_executor.margin_settings import read_margin_settings, write_margin_settings


def c(value):
    return "" if value is None else escape(str(value))


def margin_admin_box() -> str:
    vals = read_margin_settings()
    body = "<div class='box'><h2>Margin mode</h2>"
    body += "<p class='pill warn'>Safe default: disabled and dry run. Restart after changing margin settings.</p>"
    body += "<form method='post' action='/admin/margin'>"
    for key, value in vals.items():
        body += f"<label>{c(key)}</label><input type='text' name='{c(key)}' value='{c(value)}'>"
    body += "<button>Save margin settings</button></form></div>"
    return body


def save_margin_admin(body: bytes) -> None:
    posted = {k: v[-1] for k, v in parse_qs(body.decode()).items()}
    write_margin_settings(posted)

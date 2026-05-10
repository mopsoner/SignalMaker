from html import escape
from urllib.parse import parse_qs

from raspberry_executor.margin_settings import read_margin_settings, write_margin_settings


def c(value):
    return "" if value is None else escape(str(value))


def selected(current: str, value: str) -> str:
    return " selected" if current == value else ""


def checked(value: str) -> str:
    return " checked" if str(value).strip().lower() in {"1", "true", "yes", "on"} else ""


def margin_admin_box() -> str:
    vals = read_margin_settings()
    mode = vals.get("EXECUTION_MODE", "cross")
    body = "<div class='box'><h2>Execution mode</h2>"
    body += "<p class='pill ok'>Recommended: Cross Margin. Spot remains available as fallback/safety.</p>"
    body += "<form method='post' action='/admin/margin'>"
    body += "<label>EXECUTION_MODE</label>"
    body += "<select name='EXECUTION_MODE' style='width:100%;padding:10px;margin:6px 0 14px;background:#222;color:#eee;border:1px solid #444;box-sizing:border-box'>"
    body += f"<option value='cross'{selected(mode, 'cross')}>cross - primary margin mode</option>"
    body += f"<option value='isolated'{selected(mode, 'isolated')}>isolated - per-symbol margin</option>"
    body += f"<option value='spot'{selected(mode, 'spot')}>spot - no borrow</option>"
    body += "</select>"
    body += f"<label>MARGIN_DRY_RUN</label><input type='text' name='MARGIN_DRY_RUN' value='{c(vals.get('MARGIN_DRY_RUN'))}'>"
    body += f"<label>MARGIN_MAX_MULTIPLIER</label><input type='text' name='MARGIN_MAX_MULTIPLIER' value='{c(vals.get('MARGIN_MAX_MULTIPLIER'))}'>"
    body += f"<label>SHORTS_ENABLED</label><input type='text' name='SHORTS_ENABLED' value='{c(vals.get('SHORTS_ENABLED'))}'>"
    body += f"<label>MARGIN_TRANSFER_SPOT_BALANCE</label><input type='text' name='MARGIN_TRANSFER_SPOT_BALANCE' value='{c(vals.get('MARGIN_TRANSFER_SPOT_BALANCE'))}'>"
    body += "<button>Save execution mode</button></form>"
    body += "<p class='muted'>Derived automatically: MARGIN_MODE_ENABLED, MARGIN_ACCOUNT_MODE and MARGIN_ISOLATED. Restart after changing mode.</p>"
    body += "</div>"
    return body


def save_margin_admin(body: bytes) -> None:
    posted = {k: v[-1] for k, v in parse_qs(body.decode()).items()}
    write_margin_settings(posted)

from typing import Any

from raspberry_executor.candidate_cursor_store import read_candidate_cursor
from raspberry_executor.candidate_view_store import candidate_status_summary, local_candidate_rows
from raspberry_executor.env_store import public_env
from raspberry_executor.position_sync_v2 import sync_open_positions
from raspberry_executor.sqlite_db import connect
from raspberry_executor.state import StateStore

CANDIDATE_LABELS = [
    "Local state",
    "Execution state",
    "Candidate",
    "Remote",
    "Symbol",
    "Side",
    "Entry",
    "Target",
    "First seen",
    "Last seen",
    "Fingerprint",
]

CANDIDATE_KEYS = [
    "local_state",
    "execution_state",
    "candidate",
    "remote",
    "symbol",
    "side",
    "entry",
    "target",
    "first_seen",
    "last_seen",
    "fingerprint",
]

POSITION_LABELS = [
    "Status",
    "Strategy",
    "Candidate",
    "Symbol",
    "Side",
    "Qty",
    "Entry",
    "Mark",
    "PNL",
    "Target",
    "TP",
    "TP status",
    "Result",
    "Reason",
]

POSITION_KEYS = [
    "status",
    "strategy",
    "candidate",
    "symbol",
    "side",
    "qty",
    "entry",
    "mark",
    "pnl",
    "target",
    "tp",
    "tp_status",
    "result",
    "reason",
]

STATUS_LABELS = {
    "DRY_RUN": "Dry run",
    "ORDER_QUOTE_AMOUNT": "Order quote",
    "QUOTE_ASSETS": "Quote assets",
    "CANDLE_FEED_ENABLED": "Candle feed enabled",
    "CANDLE_FEED_INTERVALS": "Candle feed intervals",
    "CANDLE_FEED_POLL_SECONDS": "Candle feed poll seconds",
    "CANDLE_FEED_MAX_WORKERS": "Candle feed workers",
    "SIGNALMAKER_BASE_URL": "SignalMaker URL",
    "GATEWAY_ID": "Gateway ID",
}


def _string(value: Any) -> str:
    return "" if value is None else str(value)


def _meta_value(key: str) -> str:
    try:
        with connect() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return _string(row["value"] if row else "")
    except Exception:
        return ""


def order_status(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    if payload.get("sync_error"):
        return "sync_error"
    return _string(payload.get("status"))


def candidate_row(row: dict[str, Any]) -> dict[str, str]:
    return {
        "local_state": _string(row.get("local_status")),
        "execution_state": _string(row.get("execution_state")),
        "candidate": _string(row.get("candidate_id")),
        "remote": _string(row.get("remote_candidate_id")),
        "symbol": _string(row.get("symbol")),
        "side": _string(row.get("side")),
        "entry": _string(row.get("entry_price")),
        "target": _string(row.get("target_price")),
        "first_seen": _string(row.get("first_seen_at")),
        "last_seen": _string(row.get("last_seen_at")),
        "fingerprint": _string(row.get("signal_fingerprint")),
    }


def candidates_view(limit: int = 100) -> dict[str, Any]:
    rows = [candidate_row(row) for row in local_candidate_rows(limit=limit, include_executed=True)]
    return {
        "title": "SignalMaker Trade Candidates",
        "labels": CANDIDATE_LABELS,
        "keys": CANDIDATE_KEYS,
        "summary": candidate_status_summary(limit=max(limit, 500)),
        "rows": rows,
        "empty_message": "No local candidates.",
        "help": "Local SQLite candidates only. Local state stays received until the local candidate status changes; execution state shows whether executor consumed the signal.",
        "cursor": read_candidate_cursor(),
        "last_runtime_reset_at": _meta_value("local_runtime_data_reset_at"),
        "ignored_old_after_reset": _meta_value("local_candidates_ignored_old_after_reset"),
    }


def position_strategy(candidate_id: str, row: dict[str, Any]) -> str:
    if str(candidate_id).startswith("momentum-") or isinstance(row.get("momentum_decision"), dict) or str(row.get("strategy") or "").lower() == "momentum_rotation":
        return "momentum"
    mode = _string(row.get("mode"))
    return mode or "signal"


def position_row(candidate_id: str, row: dict[str, Any]) -> dict[str, str]:
    return {
        "status": _string(row.get("status")),
        "strategy": position_strategy(candidate_id, row),
        "candidate": _string(candidate_id),
        "symbol": _string(row.get("execution_symbol") or row.get("signal_symbol")),
        "side": _string(row.get("side")),
        "qty": _string(row.get("quantity")),
        "entry": _string(row.get("entry_price")),
        "mark": _string(row.get("mark_price")),
        "pnl": _string(row.get("unrealized_pnl") if row.get("unrealized_pnl") is not None else row.get("pnl")),
        "target": _string(row.get("target_price")),
        "tp": _string(row.get("tp_order_id")),
        "tp_status": order_status(row.get("kraken_tp_status")),
        "result": _string(row.get("close_reason") or row.get("exit_strategy") or "take_profit_only"),
        "reason": _string(row.get("close_reason")),
    }


def positions_view(limit: int = 50, sync: bool = True) -> dict[str, Any]:
    sync_result: dict[str, Any] | None = None
    sync_error = ""
    if sync:
        try:
            sync_result = sync_open_positions()
        except Exception as exc:
            sync_error = str(exc)
    state = StateStore()
    open_rows = [position_row(candidate_id, row) for candidate_id, row in state.open_positions().items()]
    closed_items = [(item.get("candidate_id", ""), item) for item in reversed(state.closed_positions()[-limit:])]
    closed_rows = [position_row(candidate_id, row) for candidate_id, row in closed_items]
    return {
        "title": "Kraken Synced Positions",
        "labels": POSITION_LABELS,
        "keys": POSITION_KEYS,
        "open_rows": open_rows,
        "closed_rows": closed_rows,
        "empty_message": "No positions.",
        "sync": sync_result or {},
        "sync_error": sync_error,
    }



SERVICE_WORKER_KEYS = ["kind", "name", "status", "detail", "pid", "started_at"]
SERVICE_WORKER_LABELS = ["Kind", "Name", "Status", "Detail", "PID", "Started at"]
LOG_KEYS = ["worker", "line"]
LOG_LABELS = ["Worker", "Line"]
KRAKEN_DIAGNOSTIC_KEYS = ["key", "value"]
KRAKEN_DIAGNOSTIC_LABELS = ["Key", "Value"]
ORDER_FILL_KEYS = ["kind", "id", "symbol", "side", "type", "quantity", "price", "status", "created_at"]
ORDER_FILL_LABELS = ["Kind", "ID", "Symbol", "Side", "Type", "Qty", "Price", "Status", "Date"]
ASSET_KEYS = ["symbol", "status", "state", "bias", "score", "rsi_15m", "updated_at"]
ASSET_LABELS = ["Symbol", "Status", "State", "Bias", "Score", "RSI 15m", "Updated"]
CANDLE_SUMMARY_KEYS = ["symbol", "timeframe", "interval", "count", "latest_open_time", "last_open_time", "updated_at"]
CANDLE_SUMMARY_LABELS = ["Symbol", "TF", "Interval", "Count", "Latest open", "Last open", "Updated"]
MOMENTUM_KEYS = ["decision_action", "execution_result", "symbol", "target_symbol", "status", "reason", "order_ids", "fill_ids", "best_asset.symbol", "best_asset.price", "best_asset.momentum_score"]
MOMENTUM_LABELS = ["Action", "Result", "Symbol", "Target", "Status", "Reason", "Order IDs", "Fill IDs", "Best asset", "Best price", "Best score"]


def _as_list(payload: Any, keys: tuple[str, ...] = ("items", "rows", "data", "results", "services", "workers", "candidates")) -> list[Any]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                return value
        return [{"name": key, **value} if isinstance(value, dict) else {"name": key, "value": value} for key, value in payload.items()]
    return [{"value": payload}]


def _summary_count(rows: list[dict[str, Any]], key: str = "status") -> dict[str, int]:
    summary: dict[str, int] = {"total": len(rows)}
    for row in rows:
        value = _string(row.get(key) or "unknown")
        summary[value] = summary.get(value, 0) + 1
    return summary


def _view(title: str, labels: list[str], keys: list[str], rows: list[dict[str, Any]], empty_message: str, summary: dict[str, Any] | None = None, errors: list[str] | None = None) -> dict[str, Any]:
    return {
        "title": title,
        "labels": labels,
        "keys": keys,
        "rows": rows,
        "summary": summary or {"total": len(rows)},
        "empty_message": empty_message,
        "errors": errors or [],
    }


def services_workers_view(services_payload: Any = None, workers_payload: Any = None, errors: list[str] | None = None) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for row in _as_list(services_payload):
        if isinstance(row, dict):
            rows.append({"kind": "service", "name": _string(row.get("name")), "status": _string(row.get("status") or row.get("state")), "detail": _string(row.get("detail") or row.get("message") or row.get("value")), "pid": _string(row.get("pid")), "started_at": _string(row.get("started_at"))})
    for row in _as_list(workers_payload):
        if isinstance(row, dict):
            rows.append({"kind": "worker", "name": _string(row.get("name")), "status": _string(row.get("status") or row.get("state")), "detail": _string(row.get("detail") or row.get("message") or row.get("value")), "pid": _string(row.get("pid")), "started_at": _string(row.get("started_at"))})
    return _view("Services / Workers", SERVICE_WORKER_LABELS, SERVICE_WORKER_KEYS, rows, "No services or workers returned.", _summary_count(rows), errors)


def logs_view(log_payloads: dict[str, Any] | None = None, errors: list[str] | None = None) -> dict[str, Any]:
    rows = []
    for worker, payload in (log_payloads or {}).items():
        lines = payload.get("lines", []) if isinstance(payload, dict) else []
        for line in lines:
            rows.append({"worker": worker, "line": _string(line)})
    return _view("Logs", LOG_LABELS, LOG_KEYS, rows, "No logs returned.", {"total": len(rows), "workers": len(log_payloads or {})}, errors)


def kraken_diagnostics_view(settings_payload: Any = None, test_payload: Any = None, errors: list[str] | None = None) -> dict[str, Any]:
    settings = settings_payload if isinstance(settings_payload, dict) else {}
    kr = settings.get("kraken", {}) if isinstance(settings.get("kraken"), dict) else {}
    ex = settings.get("executor", {}) if isinstance(settings.get("executor"), dict) else {}
    md = settings.get("market_data", {}) if isinstance(settings.get("market_data"), dict) else {}
    rows = [
        {"key": "execution_exchange", "value": _string(ex.get("execution_exchange") or "kraken")},
        {"key": "execution_mode", "value": _string(ex.get("execution_mode") or "cross")},
        {"key": "quote_assets", "value": _string(ex.get("quote_assets") or md.get("kraken_quote_assets"))},
        {"key": "kraken_rest_base", "value": _string(kr.get("kraken_rest_base") or kr.get("kraken_base_url"))},
    ]
    if isinstance(test_payload, dict):
        rows.extend({"key": f"test_{k}", "value": _string(v)} for k, v in sorted(test_payload.items()))
    return _view("Kraken Diagnostics", KRAKEN_DIAGNOSTIC_LABELS, KRAKEN_DIAGNOSTIC_KEYS, rows, "No Kraken diagnostics returned.", {"total": len(rows), "status": _string(test_payload.get("status")) if isinstance(test_payload, dict) else ""}, errors)


def orders_fills_view(orders_payload: Any = None, fills_payload: Any = None, errors: list[str] | None = None) -> dict[str, Any]:
    rows = []
    for kind, payload in (("order", orders_payload), ("fill", fills_payload)):
        for row in _as_list(payload):
            if isinstance(row, dict):
                rows.append({"kind": kind, "id": _string(row.get("order_id") or row.get("fill_id") or row.get("id")), "symbol": _string(row.get("symbol")), "side": _string(row.get("side")), "type": _string(row.get("order_type") or row.get("type")), "quantity": _string(row.get("quantity") or row.get("qty")), "price": _string(row.get("price")), "status": _string(row.get("status")), "created_at": _string(row.get("created_at") or row.get("timestamp"))})
    return _view("Orders / Fills", ORDER_FILL_LABELS, ORDER_FILL_KEYS, rows, "No orders or fills.", _summary_count(rows, "kind"), errors)


def assets_view(payload: Any = None, errors: list[str] | None = None) -> dict[str, Any]:
    rows = [{"symbol": _string(r.get("symbol")), "status": _string(r.get("status") or r.get("state")), "state": _string(r.get("state")), "bias": _string(r.get("bias")), "score": _string(r.get("score")), "rsi_15m": _string(r.get("rsi_15m")), "updated_at": _string(r.get("updated_at"))} for r in _as_list(payload) if isinstance(r, dict)]
    return _view("Assets", ASSET_LABELS, ASSET_KEYS, rows, "No assets returned.", _summary_count(rows), errors)


def market_candles_summary_view(payload: Any = None, errors: list[str] | None = None) -> dict[str, Any]:
    rows = [{key: _string(r.get(key)) for key in CANDLE_SUMMARY_KEYS} for r in _as_list(payload) if isinstance(r, dict)]
    return _view("Market Candles Summary", CANDLE_SUMMARY_LABELS, CANDLE_SUMMARY_KEYS, rows, "No candle summary returned.", {"total": len(rows), "symbols": len({r.get("symbol") for r in rows if r.get("symbol")})}, errors)


def momentum_view(payload: Any = None, errors: list[str] | None = None) -> dict[str, Any]:
    rows = []
    for r in _as_list(payload):
        if isinstance(r, dict):
            best = r.get("best_asset") if isinstance(r.get("best_asset"), dict) else {}
            rows.append({"decision_action": _string(r.get("decision_action") or r.get("action") or r.get("status")), "execution_result": _string(r.get("execution_result") or r.get("result")), "symbol": _string(r.get("symbol") or r.get("buy_symbol") or r.get("sell_symbol")), "target_symbol": _string(r.get("target_symbol") or r.get("buy_symbol") or r.get("sell_symbol") or r.get("symbol")), "status": _string(r.get("status")), "reason": _string(r.get("reason") or r.get("notes") or r.get("message")), "order_ids": _string(r.get("order_ids")), "fill_ids": _string(r.get("fill_ids")), "best_asset.symbol": _string(best.get("symbol") or r.get("symbol")), "best_asset.price": _string(best.get("price") or r.get("entry_price")), "best_asset.momentum_score": _string(best.get("momentum_score") or r.get("score"))})
    return _view("Momentum", MOMENTUM_LABELS, MOMENTUM_KEYS, rows, "No momentum rows returned.", _summary_count(rows, "decision_action"), errors)

def status_view() -> dict[str, Any]:
    env = public_env()
    rows = []
    for key, label in STATUS_LABELS.items():
        rows.append({"key": key, "label": label, "value": _string(env.get(key))})
    rows.append({"key": "candidate_cursor", "label": "Candidate cursor", "value": _string(read_candidate_cursor())})
    rows.append({"key": "last_runtime_reset_at", "label": "Last local reset", "value": _meta_value("local_runtime_data_reset_at")})
    return _view("Raspberry Executor Status", ["Key", "Label", "Value"], ["key", "label", "value"], rows, "No status values.")

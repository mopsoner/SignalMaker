import os
import time
from typing import Any

import requests

from raspberry_executor.config import load_settings
from raspberry_executor.logging_setup import setup_logging
from raspberry_executor.state import StateStore

logger = setup_logging("raspberry-momentum-decision")


def _bool(value: str | None, default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except Exception:
        return default


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return default


def _url(base_url: str, path: str) -> str:
    base = base_url.rstrip("/")
    if base.endswith("/api/v1") and path.startswith("/api/v1"):
        return f"{base}{path[len('/api/v1'):] }"
    return f"{base}{path}"


def fetch_decision() -> dict[str, Any]:
    settings = load_settings()
    params = {
        "cadence_hours": _int_env("MOMENTUM_DECISION_CADENCE_HOURS", 4),
        "starting_capital": _float_env("MOMENTUM_DECISION_STARTING_CAPITAL", 1000.0),
        "min_momentum_score": _float_env("MOMENTUM_DECISION_MIN_SCORE", 0.0),
    }
    response = requests.get(_url(settings.signalmaker_base_url, "/api/v1/momentum/decision"), params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"unexpected_momentum_decision_response:{type(data).__name__}")
    return data


def record_decision(decision: dict[str, Any]) -> None:
    action = str(decision.get("action") or "WAIT").upper()
    symbol = str(decision.get("symbol") or decision.get("buy_symbol") or decision.get("sell_symbol") or "momentum")
    StateStore().add_event("momentum-decision", "momentum_decision", {
        "action": action,
        "symbol": symbol,
        "should_trade": bool(decision.get("should_trade")),
        "buy_symbol": decision.get("buy_symbol"),
        "sell_symbol": decision.get("sell_symbol"),
        "reason": decision.get("reason"),
        "due_now": decision.get("due_now"),
        "next_check_at": decision.get("next_check_at"),
        "decision": decision,
    })


def run_once() -> dict[str, Any]:
    decision = fetch_decision()
    record_decision(decision)
    return decision


def run_loop() -> None:
    if not _bool(os.getenv("MOMENTUM_DECISION_ENABLED"), default=True):
        logger.info("momentum decision feed disabled")
        return
    poll_seconds = max(30, _int_env("MOMENTUM_DECISION_POLL_SECONDS", 60))
    logger.info("momentum decision feed started poll_seconds=%s", poll_seconds)
    while True:
        try:
            decision = run_once()
            logger.info("momentum decision action=%s symbol=%s should_trade=%s", decision.get("action"), decision.get("symbol"), decision.get("should_trade"))
        except Exception as exc:
            logger.error("momentum decision feed error=%s", str(exc))
            try:
                StateStore().add_event("momentum-decision", "momentum_decision_error", {"error": str(exc)})
            except Exception:
                pass
        time.sleep(poll_seconds)


if __name__ == "__main__":
    print(run_once())

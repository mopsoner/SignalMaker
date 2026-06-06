from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.momentum_engine import MomentumEngineRunRequest, MomentumEngineStatus
from app.services.momentum_engine_service import MomentumEngineService

router = APIRouter()


def _field(payload: dict[str, Any] | Any | None, key: str, default: Any = None) -> Any:
    if payload is None:
        return default
    if isinstance(payload, dict):
        return payload.get(key, default)
    return getattr(payload, key, default)


def _order_sequence(action: str, *, buy_symbol: str | None, sell_symbol: str | None) -> list[dict[str, str]]:
    if action == "BUY" and buy_symbol:
        return [{"type": "BUY", "symbol": buy_symbol}]
    if action == "SELL" and sell_symbol:
        return [{"type": "SELL", "symbol": sell_symbol}]
    if action == "ROTATE" and sell_symbol and buy_symbol:
        return [{"type": "SELL", "symbol": sell_symbol}, {"type": "BUY", "symbol": buy_symbol}]
    return []


def build_executor_contract(status: dict[str, Any]) -> dict[str, Any]:
    open_position = status.get("open_position")
    best_asset = status.get("best_asset")
    due_now = bool(status.get("due_now"))
    open_symbol = _field(open_position, "symbol")
    best_symbol = _field(best_asset, "symbol")
    structure_broken = bool(_field(open_position, "structure_broken"))

    action = "HOLD" if open_position else "WAIT"
    symbol = open_symbol if open_position else None
    buy_symbol = None
    sell_symbol = None
    should_trade = False

    if due_now and not open_position and best_asset:
        action = "BUY"
        buy_symbol = best_symbol
        symbol = buy_symbol
        should_trade = True
    elif due_now and open_position and structure_broken:
        sell_symbol = open_symbol
        if best_asset:
            action = "ROTATE"
            buy_symbol = best_symbol
            symbol = buy_symbol
        else:
            action = "SELL"
            symbol = sell_symbol
        should_trade = True
    elif due_now and open_position and best_asset and best_symbol != open_symbol:
        action = "ROTATE"
        sell_symbol = open_symbol
        buy_symbol = best_symbol
        symbol = buy_symbol
        should_trade = True

    buy_candidates = [best_asset] if best_asset else []
    order_sequence = _order_sequence(action, buy_symbol=buy_symbol, sell_symbol=sell_symbol)
    reason = status.get("recommendation")
    fallback_policy = {
        "enabled": True,
        "source": "/api/v1/momentum",
        "max_attempts_env": "MOMENTUM_DECISION_FALLBACK_MAX_ATTEMPTS",
    }

    return {
        "strategy": status.get("strategy"),
        "mode": "momentum_rotation",
        "source": "momentum_engine_status",
        "action": action,
        "raw_action": action,
        "symbol": symbol,
        "buy_symbol": buy_symbol,
        "sell_symbol": sell_symbol,
        "should_trade": should_trade,
        "reason": reason,
        "due_now": status.get("due_now"),
        "last_check_at": status.get("last_check_at"),
        "next_check_at": status.get("next_check_at"),
        "open_position": open_position,
        "best_asset": best_asset,
        "target_asset": best_asset,
        "buy_candidates": buy_candidates,
        "fallback_policy": fallback_policy,
        "executor_contract": {
            "action": action,
            "raw_action": action,
            "symbol": symbol,
            "buy_symbol": buy_symbol,
            "sell_symbol": sell_symbol,
            "should_trade": should_trade,
            "reason": reason,
            "order_sequence": order_sequence,
            "buy_candidates": buy_candidates,
            "fallback_policy": {
                "enabled": True,
                "source": "/api/v1/momentum",
            },
        },
        "status": status,
    }


@router.get("/status", response_model=MomentumEngineStatus)
def momentum_engine_status(
    cadence_hours: int = 4,
    starting_capital: float = 1000.0,
    min_momentum_score: float = 0.0,
    db: Session = Depends(get_db),
) -> MomentumEngineStatus:
    return MomentumEngineService(db).status(
        cadence_hours=cadence_hours,
        starting_capital=starting_capital,
        min_momentum_score=min_momentum_score,
    )


@router.get("/decision")
def momentum_engine_decision(
    cadence_hours: int = 4,
    starting_capital: float = 1000.0,
    min_momentum_score: float = 0.0,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    status = MomentumEngineService(db).status(
        cadence_hours=cadence_hours,
        starting_capital=starting_capital,
        min_momentum_score=min_momentum_score,
    )
    return build_executor_contract(status)


@router.post("/run-once", response_model=MomentumEngineStatus)
def momentum_engine_run_once(payload: MomentumEngineRunRequest, db: Session = Depends(get_db)) -> MomentumEngineStatus:
    return MomentumEngineService(db).run_once(
        force=payload.force,
        cadence_hours=payload.cadence_hours,
        starting_capital=payload.starting_capital,
        min_momentum_score=payload.min_momentum_score,
    )

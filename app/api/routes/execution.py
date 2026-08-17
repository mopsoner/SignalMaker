from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.api.deps import get_db, require_operator
from app.services.execution.kraken_execution_service import ExecutionConfigurationError, ExecutionDisabledError, KrakenExecutionService
from app.services.execution.momentum_live_execution_service import MomentumLiveExecutionService
from app.services.momentum_engine_service import MomentumEngineService

router = APIRouter(dependencies=[Depends(require_operator)])


class BuyRequest(BaseModel):
    symbol: str
    quote_amount: float | None = Field(default=None, gt=0)
    mode: Literal["spot", "margin"] = "spot"
    leverage: int | None = Field(default=None, ge=2)


class SellRequest(BaseModel):
    symbol: str
    quantity: float | None = Field(default=None, gt=0)
    mode: Literal["spot", "margin"] = "spot"
    leverage: int | None = Field(default=None, ge=2)
    intent: Literal["close_long", "reduce_long", "open_short"] = "close_long"


class CancelRequest(BaseModel):
    symbol: str
    order_id: str
    mode: Literal["spot", "margin"] = "spot"


def _call(operation):
    try:
        return operation()
    except ExecutionDisabledError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ExecutionConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/execution/kraken/buy")
def buy(request: BuyRequest, db: Session = Depends(get_db)):
    return _call(lambda: KrakenExecutionService(db).buy_market(request.symbol, request.quote_amount, mode=request.mode, leverage=request.leverage))


@router.post("/execution/kraken/sell")
def sell(request: SellRequest, db: Session = Depends(get_db)):
    return _call(lambda: KrakenExecutionService(db).sell_market(request.symbol, request.quantity, mode=request.mode, leverage=request.leverage, intent=request.intent))


@router.post("/execution/kraken/cancel")
def cancel(request: CancelRequest, db: Session = Depends(get_db)):
    return _call(lambda: KrakenExecutionService(db).cancel_order(request.symbol, request.order_id, mode=request.mode))


@router.get("/execution/kraken/account")
def account(db: Session = Depends(get_db)):
    return _call(lambda: KrakenExecutionService(db).account_summary())


@router.post("/executor/momentum/run-once")
def momentum_once(db: Session = Depends(get_db)):
    decision = MomentumEngineService(db).current_decision()
    result = _call(lambda: MomentumLiveExecutionService(db).execute_decision(decision))
    return {"decision": decision, "execution_result": result}

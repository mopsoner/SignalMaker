from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.models.momentum_backtest import MomentumBacktestRun
from app.services.momentum_backtest_service import MomentumBacktestService

router = APIRouter()


class BacktestCreatePayload(BaseModel):
    settings: dict[str, Any] | None = None


class BacktestSweepPayload(BaseModel):
    ranges: list[dict[str, float]]
    base_settings: dict[str, Any] | None = None


def _model_to_dict(row):
    if row is None:
        return None
    return {column.name: getattr(row, column.name) for column in row.__table__.columns}


@router.get("/runs")
def list_runs(limit: int = Query(default=20, ge=1, le=100), db: Session = Depends(get_db)):
    return [_model_to_dict(row) for row in MomentumBacktestService(db).list_runs(limit=limit)]


@router.post("/runs")
def create_run(payload: BacktestCreatePayload | None = None, db: Session = Depends(get_db)):
    settings = payload.settings if payload else None
    run = MomentumBacktestService(db).create_run(settings=settings)
    return _model_to_dict(run)


@router.post("/runs/rsi-sweep")
def create_rsi_sweep(payload: BacktestSweepPayload, db: Session = Depends(get_db)):
    runs = MomentumBacktestService(db).create_rsi_sweep(payload.ranges, payload.base_settings)
    return [_model_to_dict(run) for run in runs]


@router.get("/runs/latest")
def latest_run(db: Session = Depends(get_db)):
    run = MomentumBacktestService(db).latest_run()
    return _model_to_dict(run)


@router.get("/runs/{run_id}")
def get_run(run_id: str, db: Session = Depends(get_db)):
    run = db.get(MomentumBacktestRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Backtest run not found")
    return _model_to_dict(run)


@router.get("/runs/{run_id}/trades")
def trades(run_id: str, limit: int = Query(default=300, ge=1, le=2000), db: Session = Depends(get_db)):
    return [_model_to_dict(row) for row in MomentumBacktestService(db).list_trades(run_id, limit=limit)]


@router.get("/runs/{run_id}/equity")
def equity(run_id: str, limit: int = Query(default=1000, ge=10, le=5000), db: Session = Depends(get_db)):
    return [_model_to_dict(row) for row in MomentumBacktestService(db).list_equity(run_id, limit=limit)]


@router.get("/compare")
def compare(run_ids: str = Query(default=""), limit: int = Query(default=800, ge=10, le=5000), db: Session = Depends(get_db)):
    service = MomentumBacktestService(db)
    ids = [item.strip() for item in run_ids.split(",") if item.strip()]
    if not ids:
        ids = [row.run_id for row in service.list_runs(limit=5) if row.status == "completed"]
    payload = []
    for run_id in ids[:8]:
        run = db.get(MomentumBacktestRun, run_id)
        if not run:
            continue
        payload.append({
            "run": _model_to_dict(run),
            "equity": [_model_to_dict(row) for row in service.list_equity(run_id, limit=limit)],
        })
    return payload

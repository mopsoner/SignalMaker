from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.order import OrderRead
from app.services.order_service import OrderService

router = APIRouter()


@router.get("", response_model=list[OrderRead])
def list_orders(limit: int = Query(default=100, ge=1, le=1000), status: str | None = Query(default=None), db: Session = Depends(get_db)) -> list[OrderRead]:
    return OrderService(db).list_orders(limit=limit, status=status)


@router.delete("")
def clear_orders(status: str | None = Query(default=None), db: Session = Depends(get_db)) -> dict:
    deleted = OrderService(db).clear_orders(status=status)
    return {"deleted": deleted, "status": status or "all"}


@router.delete("/open")
def clear_open_orders(db: Session = Depends(get_db)) -> dict:
    deleted = OrderService(db).clear_orders(status="open")
    return {"deleted": deleted, "status": "open"}

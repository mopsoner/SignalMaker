from collections.abc import Generator

from sqlalchemy.orm import Session
from fastapi import Header, HTTPException
import hmac

from app.core.config import settings

from app.db.session import SessionLocal, rollback_and_close


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    except BaseException:
        rollback_and_close(db)
        raise
    else:
        db.close()


def require_operator(x_operator_key: str | None = Header(default=None)) -> None:
    if not x_operator_key or not hmac.compare_digest(x_operator_key, settings.admin_token):
        raise HTTPException(status_code=401, detail="operator authentication required")

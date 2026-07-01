from collections.abc import Generator
import secrets
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from app.core.config import settings
from app.services.runtime_settings import load_runtime_settings


def get_db() -> Generator[Session, None, None]:
    from app.db.session import SessionLocal

    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _runtime_admin_token(db: Session) -> str:
    try:
        runtime = load_runtime_settings(db)
        token = runtime.get("general", {}).get("admin_token") if isinstance(runtime, dict) else None
    except Exception:
        token = None
    return str(token or settings.admin_token or "")


def require_operator_key(
    x_operator_key: Annotated[str | None, Header(alias="x-operator-key")] = None,
    db: Session = Depends(get_db),
) -> None:
    expected = _runtime_admin_token(db)
    provided = str(x_operator_key or "")
    if not expected or not provided or not secrets.compare_digest(provided, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token admin invalide ou manquant",
        )

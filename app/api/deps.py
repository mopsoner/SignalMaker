from collections.abc import Generator

from sqlalchemy.orm import Session

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

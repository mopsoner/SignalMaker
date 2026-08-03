import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from app.core.config import settings

logger = logging.getLogger(__name__)

connect_args = {}
if settings.database_url.startswith("sqlite"):
    connect_args = {"check_same_thread": False}
elif settings.database_url.startswith(("postgresql", "postgres")):
    # These are guard rails, not a substitute for short transactions.  Keep the
    # values below Replit's proxy timeouts so an expensive statement fails while
    # the connection is still usable and an accidentally idle transaction is
    # released promptly.
    connect_args = {
        "options": "-c statement_timeout=120000 -c idle_in_transaction_session_timeout=60000"
    }

engine = create_engine(
    settings.database_url,
    echo=settings.sql_echo,
    future=True,
    pool_pre_ping=True,
    pool_recycle=300,
    pool_timeout=20,
    pool_size=5,
    max_overflow=5,
    connect_args=connect_args,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def rollback_and_close(db: Session) -> None:
    """Best-effort cleanup, including connections killed by PostgreSQL.

    SQLAlchemy normally invalidates a disconnected DBAPI connection itself.  A
    timeout can also happen *during* rollback, though, so cleanup must never mask
    the request/job exception that caused it.
    """
    try:
        db.rollback()
    except Exception:
        logger.warning("Database rollback failed; invalidating dead connection", exc_info=True)
        try:
            connection = db.connection()
            connection.invalidate()
        except Exception:
            pass
    finally:
        try:
            db.close()
        except Exception:
            logger.warning("Database session close failed", exc_info=True)

from contextlib import asynccontextmanager
from pathlib import Path
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.requests import Request

from app.api.router import api_router
from app.core.config import settings
from app.core.logging import configure_error_logging
from app.db.base import init_db
from app.db.session import SessionLocal, rollback_and_close
from signalmaker.market_data.repository import MarketDataRepository

_FRONTEND_DIST = Path(__file__).parent.parent / "frontend" / "dist"
error_logger = configure_error_logging()


@asynccontextmanager
async def lifespan(_: FastAPI):
    if settings.create_tables_on_boot:
        init_db()
    db = SessionLocal()
    try:
        MarketDataRepository(db).ensure_schema()
    except Exception:
        rollback_and_close(db)
        raise
    else:
        db.close()
    yield


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)


@app.middleware("http")
async def log_request_errors(request: Request, call_next):
    """Persist failed API requests so intermittent loading errors are diagnosable."""
    request_id = request.headers.get("x-request-id") or uuid4().hex
    started = perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        error_logger.exception(
            "request_failed request_id=%s method=%s path=%s duration_ms=%.1f",
            request_id,
            request.method,
            request.url.path,
            (perf_counter() - started) * 1000,
        )
        raise

    response.headers["x-request-id"] = request_id
    if response.status_code >= 500:
        error_logger.error(
            "request_failed request_id=%s method=%s path=%s status=%s duration_ms=%.1f",
            request_id,
            request.method,
            request.url.path,
            response.status_code,
            (perf_counter() - started) * 1000,
        )
    return response


@app.get("/healthz", tags=["health"])
def healthz() -> dict[str, str]:
    return {"status": "ok", "service": settings.app_name}


if _FRONTEND_DIST.is_dir():
    app.mount("/assets", StaticFiles(directory=_FRONTEND_DIST / "assets"), name="assets")

    @app.get("/{full_path:path}", include_in_schema=False)
    def serve_spa(full_path: str) -> FileResponse:
        return FileResponse(_FRONTEND_DIST / "index.html")

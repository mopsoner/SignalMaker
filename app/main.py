from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.api.router import api_router
from app.core.config import settings

_FRONTEND_DIST = Path(__file__).parent.parent / "frontend" / "dist"


@asynccontextmanager
async def lifespan(_: FastAPI):
    if settings.create_tables_on_boot:
        from app.db.base import init_db

        init_db()
    yield


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_origin_regex=settings.cors_origin_regex or None,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)


@app.get("/healthz", tags=["health"])
def healthz() -> dict[str, str]:
    return {"status": "ok", "service": settings.app_name}


if _FRONTEND_DIST.is_dir():
    app.mount("/static", StaticFiles(directory=_FRONTEND_DIST), name="static_frontend")

    @app.get("/admin", include_in_schema=False)
    @app.get("/admin/", include_in_schema=False)
    @app.get("/admin.html", include_in_schema=False)
    @app.get("/settings.html", include_in_schema=False)
    @app.get("/logs.html", include_in_schema=False)
    @app.get("/feed.html", include_in_schema=False)
    def redirect_legacy_frontend() -> RedirectResponse:
        return RedirectResponse(url="/ops.html", status_code=307)

    @app.get("/{full_path:path}", include_in_schema=False)
    def serve_static_frontend(full_path: str) -> FileResponse:
        requested = (_FRONTEND_DIST / full_path).resolve()
        if requested.is_file() and _FRONTEND_DIST.resolve() in requested.parents:
            return FileResponse(requested)
        return FileResponse(_FRONTEND_DIST / "index.html")

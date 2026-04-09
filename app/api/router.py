from fastapi import APIRouter

from app.api.routes import assets, health

api_router = APIRouter()
api_router.include_router(health.router, prefix="/api/v1", tags=["health"])
api_router.include_router(assets.router, prefix="/api/v1/assets", tags=["assets"])

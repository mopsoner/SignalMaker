from sqlalchemy import text

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.services.runtime_settings import load_runtime_settings

router = APIRouter()


@router.get('/health')
def health(db: Session = Depends(get_db)) -> dict:
    db.execute(text('SELECT 1'))
    runtime = load_runtime_settings(db)
    return {
        'status': 'ok',
        'service': runtime['general']['app_name'],
        'env': runtime['general']['app_env'],
        'database': 'ok',
    }

from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.config import settings
from app.models.app_setting import AppSetting

router = APIRouter()


DEFAULT_SETTINGS = {
    "general": {
        "app_name": settings.app_name,
        "app_env": settings.app_env,
        "cors_origins": settings.cors_origins,
        "create_tables_on_boot": settings.create_tables_on_boot,
    },
    "binance": {
        "binance_rest_base": settings.binance_rest_base,
        "binance_quote_assets": settings.binance_quote_assets,
        "binance_symbol_status": settings.binance_symbol_status,
        "binance_max_symbols": settings.binance_max_symbols,
        "binance_lookback_1m": settings.binance_lookback_1m,
        "binance_lookback_5m": settings.binance_lookback_5m,
        "binance_lookback_1h": settings.binance_lookback_1h,
        "binance_lookback_4h": settings.binance_lookback_4h,
    },
    "strategy": {
        "session_timezone_offset_hours": settings.session_timezone_offset_hours,
        "signal_rsi_period": settings.signal_rsi_period,
        "signal_swing_window": settings.signal_swing_window,
        "signal_equal_level_tolerance_pct": settings.signal_equal_level_tolerance_pct,
        "signal_overbought": settings.signal_overbought,
        "signal_oversold": settings.signal_oversold,
        "signal_price_near_extreme_pct": settings.signal_price_near_extreme_pct,
        "signal_session_confirm_filter_enabled": settings.signal_session_confirm_filter_enabled,
        "planner_min_score": settings.planner_min_score,
        "planner_min_rr": settings.planner_min_rr,
    },
}


class SettingsPayload(BaseModel):
    general: dict[str, Any]
    binance: dict[str, Any]
    strategy: dict[str, Any]


def _merged_settings(db: Session) -> dict[str, dict[str, Any]]:
    payload = {section: values.copy() for section, values in DEFAULT_SETTINGS.items()}

    rows = db.execute(select(AppSetting)).scalars().all()
    for row in rows:
        if row.category not in payload:
            payload[row.category] = {}
        payload[row.category][row.key] = row.value

    return payload


@router.get("/admin/settings")
def get_admin_settings(db: Session = Depends(get_db)) -> dict[str, dict[str, Any]]:
    return _merged_settings(db)


@router.put("/admin/settings")
def update_admin_settings(payload: SettingsPayload, db: Session = Depends(get_db)) -> dict[str, dict[str, Any]]:
    for category, values in payload.model_dump().items():
        for key, value in values.items():
            existing = db.execute(
                select(AppSetting).where(AppSetting.category == category, AppSetting.key == key)
            ).scalar_one_or_none()

            if existing is None:
                db.add(AppSetting(category=category, key=key, value=value))
            else:
                existing.value = value

    db.commit()
    return _merged_settings(db)

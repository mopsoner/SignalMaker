from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.position import Position


class RiskService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def validate_live_candidate(self, *, symbol: str, side: str, entry_price: float | None, stop_price: float | None, target_price: float | None, quantity: float) -> None:
        if not settings.live_trading_enabled:
            raise RuntimeError('Live trading is disabled')
        if side == 'short' and not settings.live_spot_allow_shorts:
            raise RuntimeError('Live short trading is disabled for current Binance spot mode')
        if entry_price is None:
            raise RuntimeError('Missing entry price')
        if settings.live_require_tp_sl and (stop_price is None or target_price is None):
            raise RuntimeError('TP/SL are required for live trading')
        if stop_price is not None and target_price is not None:
            if side == 'long':
                if not (stop_price < entry_price < target_price):
                    raise RuntimeError('Invalid long risk ladder: stop < entry < target required')
            else:
                if not (target_price < entry_price < stop_price):
                    raise RuntimeError('Invalid short risk ladder: target < entry < stop required')

        open_positions = self.db.execute(select(func.count()).select_from(Position).where(Position.status == 'open')).scalar_one()
        if int(open_positions or 0) >= settings.live_max_open_positions:
            raise RuntimeError('Max open positions reached')

        notional = float(entry_price) * float(quantity)
        if notional > float(settings.live_max_notional_per_trade):
            raise RuntimeError(f'Notional {notional:.2f} exceeds max per trade {settings.live_max_notional_per_trade:.2f}')

from datetime import datetime, timezone

from app.services.runtime_settings import get_runtime_signal_config
from app.strategy.legacy_engine import build_signal


class SignalEngineService:
    def heartbeat(self) -> dict:
        return {
            'service': 'signal_engine',
            'status': 'ready',
            'last_tick_at': datetime.now(timezone.utc).isoformat(),
            'strategy': 'legacy_wyckoff_v231',
        }

    def compute_signal(self, symbol: str, candles: dict[str, list[dict]]) -> dict:
        cfg = get_runtime_signal_config()
        signal = build_signal(symbol, candles['1m'], candles['5m'], candles['1h'], candles['4h'], cfg)
        signal['engine_name'] = 'legacy_wyckoff_v231'
        return signal

"""Provider-neutral contracts used by the momentum rotation engine."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Protocol, Any


class RankingLoader(Protocol):
    def __call__(self, limit: int) -> list[dict[str, Any]]: ...


class CandleLoader(Protocol):
    def __call__(self, symbol: str) -> tuple[float, str] | None: ...


@dataclass(frozen=True)
class MomentumMarketContext:
    """Everything which varies between markets, explicitly supplied by callers."""

    market_scope: str
    reference_currency: str = "USD"
    max_positions: int = 1
    market_is_open: Callable[[datetime], bool] = lambda _now: True

    def is_open_now(self) -> bool:
        return self.market_is_open(datetime.now(timezone.utc))


CRYPTO_CONTEXT = MomentumMarketContext(market_scope="crypto", reference_currency="USDC")


def weekday_market_hours(now: datetime) -> bool:
    """Default EU/US session envelope; deployments can inject an exchange calendar."""
    return now.weekday() < 5 and 8 <= now.hour < 21


STOCK_ETF_CONTEXT = MomentumMarketContext(
    market_scope="stock_etf",
    reference_currency="EUR",
    max_positions=1,
    market_is_open=weekday_market_hours,
)

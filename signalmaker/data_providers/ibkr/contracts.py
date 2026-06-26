from dataclasses import dataclass
from typing import Optional

from ib_async import Stock

from .errors import IBKRContractNotFoundError


@dataclass
class ResolvedIBKRContract:
    symbol: str
    sec_type: str
    exchange: str
    primary_exchange: Optional[str]
    currency: str
    conid: int
    local_symbol: Optional[str]
    trading_class: Optional[str]
    ambiguous: bool


class IBKRContractResolver:
    def __init__(self, ib):
        self.ib = ib

    async def resolve_stock_or_etf(
        self,
        symbol: str,
        currency: str,
        exchange: str = "SMART",
        primary_exchange: Optional[str] = None,
    ) -> ResolvedIBKRContract:
        contract = Stock(symbol, exchange, currency)

        if primary_exchange:
            contract.primaryExchange = primary_exchange

        details = await self.ib.reqContractDetailsAsync(contract)

        if not details:
            raise IBKRContractNotFoundError(
                f"No IBKR contract found for symbol={symbol}, exchange={exchange}, currency={currency}"
            )

        ambiguous = len(details) > 1
        selected = self._select_best_contract(details, primary_exchange)

        c = selected.contract

        return ResolvedIBKRContract(
            symbol=c.symbol,
            sec_type=c.secType,
            exchange=c.exchange,
            primary_exchange=getattr(c, "primaryExchange", None),
            currency=c.currency,
            conid=c.conId,
            local_symbol=getattr(c, "localSymbol", None),
            trading_class=getattr(c, "tradingClass", None),
            ambiguous=ambiguous,
        )

    def _select_best_contract(self, details, primary_exchange: Optional[str]):
        if primary_exchange:
            for detail in details:
                if getattr(detail.contract, "primaryExchange", None) == primary_exchange:
                    return detail

        return details[0]

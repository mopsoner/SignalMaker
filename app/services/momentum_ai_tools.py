"""Validated read tools: no SQL supplied by the model, no execution paths."""
from datetime import datetime, timedelta, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from app.models.market_candle import MarketCandle
from app.models.momentum_engine import MomentumEnginePosition, MomentumEngineTrade
from app.models.momentum_engine_current_decision import MomentumEngineDecisionHistory
from app.models.order import Order
from app.services.momentum_engine_service import MomentumEngineService
from app.services.momentum_service import MomentumService


class ReadRequest(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    dataset: Literal['positions', 'trades', 'orders', 'decisions', 'ranking', 'current_decision', 'candles']
    symbol: str | None = Field(default=None, max_length=32, pattern=r'^[A-Za-z0-9/_.:-]+$')
    interval: Literal['15m', '1h', '4h'] = '15m'
    before: str | None = Field(default=None, max_length=40)
    limit: int = Field(default=50, ge=1, le=200)
    offset: int = Field(default=0, ge=0, le=5000)


TOOL = {
    'type': 'function', 'name': 'read_momentum_data',
    'description': 'Read existing SignalMaker services and database. Paginate positions, trades, orders and decision history; retrieve ranking/current decision or closed Kraken candles before an ISO timestamp. Trades/orders/decisions are limited to the selected history window. Candles can go further back. No execution or write access.',
    'parameters': ReadRequest.model_json_schema(), 'strict': False,
}


class MomentumReadTools:
    def __init__(self, db, as_of, days):
        self.db = db
        self.as_of = datetime.fromisoformat(as_of)
        self.since = self.as_of - timedelta(days=days)

    def read(self, arguments):
        request = ReadRequest.model_validate(arguments)
        cutoff = self.as_of
        if request.before:
            parsed = datetime.fromisoformat(request.before.replace('Z', '+00:00'))
            if parsed.tzinfo is None:
                raise ValueError('before must include a timezone')
            cutoff = min(cutoff, parsed.astimezone(timezone.utc))
        kind = request.dataset
        if kind == 'ranking':
            rows = MomentumService(self.db).list_rankings(limit=min(request.offset + request.limit + 1, 5201))
            if request.symbol:
                rows = [r for r in rows if r.get('symbol') == request.symbol.upper()]
            return self._page(rows[request.offset:], request)
        if kind == 'current_decision':
            return {'data': MomentumEngineService(self.db).current_decision(), 'note': 'Latest persisted decision; not a historical reconstruction.'}
        if kind == 'candles':
            if not request.symbol:
                raise ValueError('symbol is required for candles')
            model, timestamp = MarketCandle, MarketCandle.close_time
            statement = select(model).where(model.symbol == request.symbol.upper(), model.interval == request.interval,
                model.provider == 'KRAKEN', model.closed_at(cutoff))
            names = 'candle_id symbol interval close_time open high low close volume'
        elif kind == 'orders':
            model, timestamp = Order, Order.created_at
            ids = select(MomentumEnginePosition.position_id).where(
                MomentumEnginePosition.market_scope == 'crypto', MomentumEnginePosition.strategy == 'momentum_rotation_v1')
            statement = select(model).where(model.position_id.in_(ids), timestamp >= self.since, timestamp <= cutoff)
            names = 'order_id position_id symbol side order_type status quantity requested_price filled_price created_at'
        elif kind == 'decisions':
            model, timestamp = MomentumEngineDecisionHistory, MomentumEngineDecisionHistory.produced_at
            statement = select(model).where(model.market_scope == 'crypto', timestamp >= self.since, timestamp <= cutoff)
            names = 'decision_id produced_at payload_json'
        else:
            model = MomentumEnginePosition if kind == 'positions' else MomentumEngineTrade
            timestamp = model.opened_at if kind == 'positions' else model.created_at
            statement = select(model).where(model.market_scope == 'crypto', model.strategy == 'momentum_rotation_v1', timestamp <= cutoff)
            if kind == 'trades':
                statement = statement.where(timestamp >= self.since)
            names = ('position_id symbol status quantity entry_price mark_price entry_score entry_rank unrealized_pnl opened_at closed_at'
                     if kind == 'positions' else 'trade_id symbol action price quantity pnl reason created_at')
        if request.symbol and kind != 'candles':
            if kind == 'decisions':
                raise ValueError('Symbol filter is unavailable for decision payloads; query the decision pages instead.')
            statement = statement.where(model.symbol == request.symbol.upper())
        rows = list(self.db.scalars(statement.order_by(timestamp.desc(), *model.__table__.primary_key.columns).offset(request.offset).limit(request.limit + 1)))
        return self._page([{name: getattr(row, name) for name in names.split()} for row in rows], request)

    @staticmethod
    def _page(rows, request):
        more = len(rows) > request.limit
        return {'items': rows[:request.limit], 'has_more': more,
                'next_offset': request.offset + request.limit if more else None,
                'note': 'Current position/order state and latest ranking are not historical state reconstructions.'}

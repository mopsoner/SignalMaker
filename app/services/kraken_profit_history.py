"""Incremental realized PnL: writes once per close, reads daily totals only."""
from datetime import datetime, timedelta, timezone
import math
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from app.models.kraken_daily_profit import KrakenDailyProfit, KrakenProfitContribution

PARIS = ZoneInfo('Europe/Paris')


def record_profit(db, row):
    profit = (row.meta or {}).get('realized_profit') or {}
    value, currency = profit.get('value'), profit.get('currency')
    if ((row.meta or {}).get('execution_mode') != 'live' or row.status != 'closed' or not row.closed_at
            or profit.get('verified') is not True or currency not in {'USD', 'USDC'}
            or not isinstance(value, (int, float)) or not math.isfinite(value)):
        return
    closed = row.closed_at
    if closed.tzinfo is None:
        closed = closed.replace(tzinfo=timezone.utc)
    day = closed.astimezone(PARIS).date().isoformat()
    insert = sqlite_insert if db.get_bind().dialect.name == 'sqlite' else pg_insert
    # The ledger insert and aggregate increment commit in the close transaction.
    inserted = db.execute(insert(KrakenProfitContribution).values(
        position_id=row.position_id, day=day, symbol=row.symbol,
        currency=currency, value=value).on_conflict_do_nothing(
            index_elements=['position_id']))
    if inserted.rowcount != 1:
        return
    db.execute(insert(KrakenDailyProfit).values(day=day, symbol=row.symbol,
        currency=currency, value=value, closed_count=1).on_conflict_do_update(
            index_elements=['day', 'symbol', 'currency'], set_={
                'value': KrakenDailyProfit.value + value,
                'closed_count': KrakenDailyProfit.closed_count + 1}))


def daily_profit(db, *, days=30, now=None):
    today = (now or datetime.now(timezone.utc)).astimezone(PARIS).date()
    first = today - timedelta(days=days - 1)
    dates = [(first + timedelta(days=i)).isoformat() for i in range(days)]
    totals, count = {}, 0
    rows = db.scalars(select(KrakenDailyProfit).where(
        KrakenDailyProfit.day >= first.isoformat(), KrakenDailyProfit.day <= today.isoformat()))
    for row in rows:
        totals.setdefault((row.currency, row.symbol), {})[row.day] = row.value
        count += row.closed_count
    return {'days': days, 'timezone': 'Europe/Paris', 'dates': dates,
            'series': [{'symbol': symbol, 'currency': currency,
                        'values': [round(values.get(day, 0), 8) for day in dates]}
                       for (currency, symbol), values in sorted(totals.items())],
            'closedCount': count, 'feesIncluded': False}

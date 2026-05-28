from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.momentum_engine import MomentumEnginePosition, MomentumEngineTrade
from app.services.momentum_service import MomentumService


class MomentumEngineService:
    """Dedicated backend paper engine for momentum rotation.

    It is deliberately separate from the real executor and existing positions table.
    The engine persists its paper positions/trades in dedicated tables.
    """

    STRATEGY = "momentum_rotation_v1"
    MODE = "paper"

    def __init__(self, db: Session) -> None:
        self.db = db

    def status(self, *, cadence_hours: int = 4, starting_capital: float = 1000.0, min_momentum_score: float = 0.0) -> dict[str, Any]:
        self._ensure_tables()
        best_asset = self._best_asset(min_momentum_score=min_momentum_score)
        open_position = self._open_position()
        last_trade = self._last_check_trade()
        now = datetime.now(timezone.utc)
        last_check_at = last_trade.created_at if last_trade else None
        next_check_at = last_check_at + timedelta(hours=cadence_hours) if last_check_at else None
        due_now = next_check_at is None or now >= next_check_at
        cash = self._cash_balance(starting_capital=starting_capital)

        if open_position:
            mark_price = self._price_for(open_position.symbol, fallback=open_position.entry_price)
            open_position.mark_price = mark_price
            open_position.unrealized_pnl = (open_position.quantity * mark_price) - open_position.entry_value
            self.db.commit()

        equity = cash + self._open_position_value(open_position)
        total_pnl = equity - starting_capital
        recommendation = self._recommendation(open_position=open_position, best_asset=best_asset, due_now=due_now)

        return {
            "strategy": self.STRATEGY,
            "mode": self.MODE,
            "cadence_hours": cadence_hours,
            "starting_capital": starting_capital,
            "cash": round(cash, 8),
            "equity": round(equity, 8),
            "total_pnl": round(total_pnl, 8),
            "total_pnl_pct": round((total_pnl / starting_capital) * 100, 4) if starting_capital else 0.0,
            "open_position": open_position,
            "best_asset": best_asset,
            "last_check_at": last_check_at,
            "next_check_at": next_check_at,
            "due_now": due_now,
            "recommendation": recommendation,
            "trades": self._recent_trades(limit=50),
        }

    def run_once(self, *, force: bool = False, cadence_hours: int = 4, starting_capital: float = 1000.0, min_momentum_score: float = 0.0) -> dict[str, Any]:
        self._ensure_tables()
        before = self.status(cadence_hours=cadence_hours, starting_capital=starting_capital, min_momentum_score=min_momentum_score)
        if not force and not before["due_now"]:
            return before

        best_asset = before["best_asset"]
        if not best_asset:
            self._record_trade(action="CHECK_NO_ELIGIBLE_ASSET", symbol="NONE", price=0.0, quantity=0.0, value=0.0, pnl=0.0, reason="No asset above minimum momentum score")
            self.db.commit()
            return self.status(cadence_hours=cadence_hours, starting_capital=starting_capital, min_momentum_score=min_momentum_score)

        open_position = self._open_position()
        if open_position and open_position.symbol != best_asset["symbol"]:
            self._close_position(open_position, reason=f"Rotate into {best_asset['symbol']} with stronger momentum")
            open_position = None

        if open_position and open_position.symbol == best_asset["symbol"]:
            price = float(best_asset.get("price") or open_position.entry_price)
            self._record_trade(action="HOLD_TOP_MOMENTUM", symbol=open_position.symbol, price=price, quantity=open_position.quantity, value=open_position.quantity * price, pnl=0.0, reason="Current paper position still has the best eligible momentum")
        elif open_position is None:
            cash = self._cash_balance(starting_capital=starting_capital)
            if cash > 0 and float(best_asset.get("price") or 0) > 0:
                self._open_new_position(best_asset, cash)
            else:
                self._record_trade(action="CHECK_NO_CASH", symbol=best_asset["symbol"], price=float(best_asset.get("price") or 0), quantity=0.0, value=0.0, pnl=0.0, reason="No cash available for momentum rotation")

        self.db.commit()
        return self.status(cadence_hours=cadence_hours, starting_capital=starting_capital, min_momentum_score=min_momentum_score)

    def _ensure_tables(self) -> None:
        from app.models.base import Base
        from app.db.session import engine

        Base.metadata.create_all(bind=engine)

    def _best_asset(self, *, min_momentum_score: float) -> dict[str, Any] | None:
        rows = MomentumService(self.db).list_rankings(limit=300)
        for row in rows:
            if float(row.get("price") or 0) > 0 and float(row.get("momentum_score") or 0) > min_momentum_score:
                return row
        return None

    def _open_position(self) -> MomentumEnginePosition | None:
        stmt = (
            select(MomentumEnginePosition)
            .where(MomentumEnginePosition.strategy == self.STRATEGY, MomentumEnginePosition.status == "open")
            .order_by(MomentumEnginePosition.opened_at.desc())
            .limit(1)
        )
        return self.db.scalars(stmt).first()

    def _recent_trades(self, *, limit: int = 50) -> list[MomentumEngineTrade]:
        stmt = (
            select(MomentumEngineTrade)
            .where(MomentumEngineTrade.strategy == self.STRATEGY)
            .order_by(MomentumEngineTrade.created_at.desc())
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def _last_check_trade(self) -> MomentumEngineTrade | None:
        stmt = (
            select(MomentumEngineTrade)
            .where(MomentumEngineTrade.strategy == self.STRATEGY)
            .order_by(MomentumEngineTrade.created_at.desc())
            .limit(1)
        )
        return self.db.scalars(stmt).first()

    def _cash_balance(self, *, starting_capital: float) -> float:
        trades = list(self.db.scalars(select(MomentumEngineTrade).where(MomentumEngineTrade.strategy == self.STRATEGY)).all())
        cash = starting_capital
        for trade in trades:
            if trade.action.startswith("BUY"):
                cash -= float(trade.value or 0)
            if trade.action.startswith("SELL"):
                cash += float(trade.value or 0)
        return cash

    def _open_position_value(self, position: MomentumEnginePosition | None) -> float:
        if not position:
            return 0.0
        mark_price = position.mark_price or self._price_for(position.symbol, fallback=position.entry_price)
        return float(position.quantity or 0) * float(mark_price or 0)

    def _price_for(self, symbol: str, *, fallback: float) -> float:
        rows = MomentumService(self.db).list_rankings(limit=300)
        for row in rows:
            if row.get("symbol") == symbol and row.get("price"):
                return float(row["price"])
        return float(fallback or 0)

    def _open_new_position(self, asset: dict[str, Any], cash: float) -> MomentumEnginePosition:
        price = float(asset["price"])
        quantity = cash / price
        now = datetime.now(timezone.utc)
        position = MomentumEnginePosition(
            position_id=f"mompos-{uuid4().hex}",
            strategy=self.STRATEGY,
            symbol=asset["symbol"],
            status="open",
            quantity=quantity,
            entry_price=price,
            entry_value=cash,
            entry_score=float(asset.get("momentum_score") or 0),
            entry_rank=int(asset.get("rank") or 0),
            mark_price=price,
            unrealized_pnl=0.0,
            meta={"classification": asset.get("classification"), "data_quality": asset.get("data_quality")},
            opened_at=now,
        )
        self.db.add(position)
        self._record_trade(action="BUY_TOP_MOMENTUM", symbol=asset["symbol"], price=price, quantity=quantity, value=cash, pnl=0.0, reason=f"Top eligible momentum rank #{asset.get('rank')}")
        return position

    def _close_position(self, position: MomentumEnginePosition, *, reason: str) -> None:
        price = self._price_for(position.symbol, fallback=position.entry_price)
        value = position.quantity * price
        pnl = value - position.entry_value
        position.status = "closed"
        position.mark_price = price
        position.unrealized_pnl = pnl
        position.closed_at = datetime.now(timezone.utc)
        self._record_trade(action="SELL_ROTATE", symbol=position.symbol, price=price, quantity=position.quantity, value=value, pnl=pnl, reason=reason)

    def _record_trade(self, *, action: str, symbol: str, price: float, quantity: float, value: float, pnl: float, reason: str) -> MomentumEngineTrade:
        trade = MomentumEngineTrade(
            trade_id=f"momtrade-{uuid4().hex}",
            strategy=self.STRATEGY,
            action=action,
            symbol=symbol,
            price=float(price or 0),
            quantity=float(quantity or 0),
            value=float(value or 0),
            pnl=float(pnl or 0),
            reason=reason,
            meta={},
            created_at=datetime.now(timezone.utc),
        )
        self.db.add(trade)
        return trade

    def _recommendation(self, *, open_position: MomentumEnginePosition | None, best_asset: dict[str, Any] | None, due_now: bool) -> str:
        if not best_asset:
            return "No eligible asset above minimum momentum score."
        if not open_position:
            return f"Buy {best_asset['symbol']} on next run."
        if open_position.symbol != best_asset["symbol"]:
            return f"Rotate from {open_position.symbol} to {best_asset['symbol']}."
        return "Hold current momentum leader." if due_now else "Wait until next scheduled momentum check."

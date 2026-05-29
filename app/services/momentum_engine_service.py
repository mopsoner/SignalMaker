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

    Strategy v2:
    - Buy the best eligible momentum asset with valid 15m structure.
    - Hold it even if another asset becomes #1.
    - Sell only when the current asset breaks 15m structure bearish.
    - After a break, rotate into the next best eligible asset with valid 15m structure.
    """

    STRATEGY = "momentum_rotation_v1"
    MODE = "paper"
    VALID_STRUCTURE_STATUSES = {"valid", "valid_bullish"}
    BROKEN_STRUCTURE_STATUSES = {"broken_bearish"}

    def __init__(self, db: Session) -> None:
        self.db = db

    def status(self, *, cadence_hours: int = 4, starting_capital: float = 1000.0, min_momentum_score: float = 0.0) -> dict[str, Any]:
        rankings = self._rankings()
        return self._build_status(
            rankings=rankings,
            cadence_hours=cadence_hours,
            starting_capital=starting_capital,
            min_momentum_score=min_momentum_score,
        )

    def run_once(self, *, force: bool = False, cadence_hours: int = 4, starting_capital: float = 1000.0, min_momentum_score: float = 0.0) -> dict[str, Any]:
        rankings = self._rankings()
        before = self._build_status(
            rankings=rankings,
            cadence_hours=cadence_hours,
            starting_capital=starting_capital,
            min_momentum_score=min_momentum_score,
        )
        if not force and not before["due_now"]:
            return before

        open_position = self._open_position()
        current_asset = self._asset_for(open_position.symbol, rankings=rankings) if open_position else None
        current_broken = self._structure_broken(current_asset) if current_asset else False

        if open_position and not current_broken:
            price = self._price_for(open_position.symbol, rankings=rankings, fallback=open_position.entry_price)
            self._record_trade(
                action="HOLD_STRUCTURE_VALID",
                symbol=open_position.symbol,
                price=price,
                quantity=open_position.quantity,
                value=open_position.quantity * price,
                pnl=0.0,
                reason=self._hold_reason(current_asset),
            )
            self.db.commit()
            return self._build_status(
                rankings=rankings,
                cadence_hours=cadence_hours,
                starting_capital=starting_capital,
                min_momentum_score=min_momentum_score,
            )

        exclude_symbols: set[str] = set()
        if open_position and current_broken:
            exclude_symbols.add(open_position.symbol)
            self._close_position(
                open_position,
                rankings=rankings,
                reason=self._break_reason(current_asset) or "15m structure broken bearish",
            )
            open_position = None

        best_asset = self._best_eligible_asset(rankings=rankings, min_momentum_score=min_momentum_score, exclude_symbols=exclude_symbols)
        if not best_asset:
            self._record_trade(action="CHECK_NO_ELIGIBLE_STRUCTURE_ASSET", symbol="NONE", price=0.0, quantity=0.0, value=0.0, pnl=0.0, reason="No momentum asset with valid 15m structure")
            self.db.commit()
            return self._build_status(
                rankings=rankings,
                cadence_hours=cadence_hours,
                starting_capital=starting_capital,
                min_momentum_score=min_momentum_score,
            )

        cash = self._cash_balance(starting_capital=starting_capital)
        if cash > 0 and float(best_asset.get("price") or 0) > 0:
            self._open_new_position(best_asset, cash)
        else:
            self._record_trade(action="CHECK_NO_CASH", symbol=best_asset["symbol"], price=float(best_asset.get("price") or 0), quantity=0.0, value=0.0, pnl=0.0, reason="No cash available for momentum structure rotation")

        self.db.commit()
        return self._build_status(
            rankings=rankings,
            cadence_hours=cadence_hours,
            starting_capital=starting_capital,
            min_momentum_score=min_momentum_score,
        )

    def _rankings(self) -> list[dict[str, Any]]:
        return MomentumService(self.db).list_rankings(limit=300)

    def _build_status(self, *, rankings: list[dict[str, Any]], cadence_hours: int, starting_capital: float, min_momentum_score: float) -> dict[str, Any]:
        best_asset = self._best_eligible_asset(rankings=rankings, min_momentum_score=min_momentum_score, exclude_symbols=set())
        open_position = self._open_position()
        current_asset = self._asset_for(open_position.symbol, rankings=rankings) if open_position else None
        open_position_payload = self._position_payload(open_position, rankings=rankings, current_asset=current_asset) if open_position else None
        last_trade = self._last_check_trade()
        now = datetime.now(timezone.utc)
        last_check_at = last_trade.created_at if last_trade else None
        next_check_at = last_check_at + timedelta(hours=cadence_hours) if last_check_at else None
        due_now = next_check_at is None or now >= next_check_at
        cash = self._cash_balance(starting_capital=starting_capital)
        equity = cash + self._open_position_value(open_position, rankings=rankings)
        total_pnl = equity - starting_capital
        recommendation = self._recommendation(open_position=open_position, current_asset=current_asset, best_asset=best_asset, due_now=due_now)

        return {
            "strategy": self.STRATEGY,
            "mode": self.MODE,
            "cadence_hours": cadence_hours,
            "starting_capital": starting_capital,
            "cash": round(cash, 8),
            "equity": round(equity, 8),
            "total_pnl": round(total_pnl, 8),
            "total_pnl_pct": round((total_pnl / starting_capital) * 100, 4) if starting_capital else 0.0,
            "open_position": open_position_payload,
            "best_asset": best_asset,
            "last_check_at": last_check_at,
            "next_check_at": next_check_at,
            "due_now": due_now,
            "recommendation": recommendation,
            "trades": self._recent_trades(limit=50),
        }

    def _asset_for(self, symbol: str, *, rankings: list[dict[str, Any]]) -> dict[str, Any] | None:
        for row in rankings:
            if row.get("symbol") == symbol:
                return row
        return None

    def _structure_valid(self, asset: dict[str, Any] | None) -> bool:
        if not asset:
            return False
        if asset.get("structure_15m_status") in self.BROKEN_STRUCTURE_STATUSES:
            return False
        if asset.get("mss_15m_bearish") or asset.get("bos_15m_bearish"):
            return False
        return asset.get("structure_15m_status") in self.VALID_STRUCTURE_STATUSES

    def _structure_broken(self, asset: dict[str, Any] | None) -> bool:
        if not asset:
            return True
        return (
            asset.get("structure_15m_status") in self.BROKEN_STRUCTURE_STATUSES
            or bool(asset.get("mss_15m_bearish"))
            or bool(asset.get("bos_15m_bearish"))
        )

    def _best_eligible_asset(self, *, rankings: list[dict[str, Any]], min_momentum_score: float, exclude_symbols: set[str]) -> dict[str, Any] | None:
        for row in rankings:
            if row.get("symbol") in exclude_symbols:
                continue
            if float(row.get("price") or 0) <= 0:
                continue
            if float(row.get("momentum_score") or 0) <= min_momentum_score:
                continue
            if not self._structure_valid(row):
                continue
            return row
        return None

    def _hold_reason(self, asset: dict[str, Any] | None) -> str:
        if not asset:
            return "Current asset not found in ranking, holding until explicit structure break."
        return f"15m structure holding: {asset.get('structure_15m_status')} / {asset.get('structure_reason')}"

    def _break_reason(self, asset: dict[str, Any] | None) -> str | None:
        if not asset:
            return "Current asset missing from ranking snapshot"
        return asset.get("structure_reason") or "15m structure broken bearish"

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

    def _position_payload(self, position: MomentumEnginePosition, *, rankings: list[dict[str, Any]], current_asset: dict[str, Any] | None) -> dict[str, Any]:
        mark_price = self._price_for(position.symbol, rankings=rankings, fallback=position.entry_price)
        unrealized_pnl = (float(position.quantity or 0) * mark_price) - float(position.entry_value or 0)
        return {
            "position_id": position.position_id,
            "symbol": position.symbol,
            "status": position.status,
            "quantity": position.quantity,
            "entry_price": position.entry_price,
            "entry_value": position.entry_value,
            "entry_score": position.entry_score,
            "entry_rank": position.entry_rank,
            "mark_price": mark_price,
            "unrealized_pnl": unrealized_pnl,
            "opened_at": position.opened_at,
            "closed_at": position.closed_at,
            "structure_15m_status": current_asset.get("structure_15m_status") if current_asset else "missing",
            "structure_15m_bias": current_asset.get("structure_15m_bias") if current_asset else "missing",
            "structure_reason": current_asset.get("structure_reason") if current_asset else "asset_missing_from_ranking",
            "structure_broken": self._structure_broken(current_asset),
        }

    def _open_position_value(self, position: MomentumEnginePosition | None, *, rankings: list[dict[str, Any]]) -> float:
        if not position:
            return 0.0
        mark_price = self._price_for(position.symbol, rankings=rankings, fallback=position.entry_price)
        return float(position.quantity or 0) * float(mark_price or 0)

    def _price_for(self, symbol: str, *, rankings: list[dict[str, Any]], fallback: float) -> float:
        for row in rankings:
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
            meta={
                "classification": asset.get("classification"),
                "data_quality": asset.get("data_quality"),
                "structure_15m_status": asset.get("structure_15m_status"),
                "structure_reason": asset.get("structure_reason"),
            },
            opened_at=now,
        )
        self.db.add(position)
        self._record_trade(action="BUY_TOP_VALID_STRUCTURE", symbol=asset["symbol"], price=price, quantity=quantity, value=cash, pnl=0.0, reason=f"Top momentum rank #{asset.get('rank')} with valid 15m structure")
        return position

    def _close_position(self, position: MomentumEnginePosition, *, rankings: list[dict[str, Any]], reason: str) -> None:
        price = self._price_for(position.symbol, rankings=rankings, fallback=position.entry_price)
        value = position.quantity * price
        pnl = value - position.entry_value
        position.status = "closed"
        position.mark_price = price
        position.unrealized_pnl = pnl
        position.closed_at = datetime.now(timezone.utc)
        self._record_trade(action="SELL_STRUCTURE_BREAK", symbol=position.symbol, price=price, quantity=position.quantity, value=value, pnl=pnl, reason=reason)

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

    def _recommendation(self, *, open_position: MomentumEnginePosition | None, current_asset: dict[str, Any] | None, best_asset: dict[str, Any] | None, due_now: bool) -> str:
        if open_position and not self._structure_broken(current_asset):
            return f"Hold {open_position.symbol} until 15m structure breaks."
        if open_position and self._structure_broken(current_asset):
            target = best_asset.get("symbol") if best_asset else "cash"
            return f"Sell {open_position.symbol}: 15m structure broken. Rotate to {target}."
        if not best_asset:
            return "No eligible momentum asset with valid 15m structure."
        return f"Buy {best_asset['symbol']} — best momentum with valid 15m structure." if due_now else "Wait until next scheduled momentum check."

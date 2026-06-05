from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from app.models.market_candle import MarketCandle
from app.models.momentum_engine import MomentumEnginePosition, MomentumEngineTrade
from app.services.momentum_service import MomentumService


class MomentumEngineService:
    """Dedicated backend paper engine for momentum rotation.

    Strategy v3:
    - Exit risk is still managed by bearish 15m structure.
    - Entry timing is based on RSI 1h pullback/recovery zone.
    - The engine looks for entry-ready assets inside the top momentum pool.
    - A current position is sold only if 15m structure breaks or another top-pool asset has a valid entry.
    """

    STRATEGY = "momentum_rotation_v1"
    MODE = "paper"
    VALID_STRUCTURE_STATUSES = MomentumService.VALID_STRUCTURE_STATUSES
    BROKEN_STRUCTURE_STATUSES = MomentumService.BROKEN_STRUCTURE_STATUSES
    ENTRY_RSI_MIN = MomentumService.ENTRY_RSI_MIN
    ENTRY_RSI_MAX = MomentumService.ENTRY_RSI_MAX
    ENTRY_RSI_FIELD = MomentumService.ENTRY_RSI_FIELD
    ENTRY_RSI_TIMEFRAME = MomentumService.ENTRY_RSI_TIMEFRAME
    ENTRY_POOL_TOP_N = MomentumService.ENTRY_POOL_TOP_N
    ENTRY_POOL_MIN_LEADER_RATIO = MomentumService.ENTRY_POOL_MIN_LEADER_RATIO

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
        exclude_symbols = {open_position.symbol} if open_position else set()
        next_entry = self._best_entry_ready_asset(
            rankings=rankings,
            min_momentum_score=min_momentum_score,
            exclude_symbols=exclude_symbols,
        )

        if open_position:
            if current_broken:
                self._close_position(
                    open_position,
                    rankings=rankings,
                    reason=self._break_reason(current_asset) or "15m structure broken bearish",
                )
                if next_entry:
                    cash = self._cash_balance(starting_capital=starting_capital)
                    if cash > 0 and float(next_entry.get("price") or 0) > 0:
                        self._open_new_position(next_entry, cash, action="BUY_AFTER_STRUCTURE_BREAK")
                    else:
                        self._record_trade(action="CHECK_NO_CASH", symbol=next_entry["symbol"], price=float(next_entry.get("price") or 0), quantity=0.0, value=0.0, pnl=0.0, reason="No cash available after 15m structure break")
                else:
                    self._record_trade(action="STAY_CASH_AFTER_STRUCTURE_BREAK", symbol="NONE", price=0.0, quantity=0.0, value=0.0, pnl=0.0, reason="15m structure broke and no top-pool asset has RSI 1h entry ready")
            elif next_entry:
                self._close_position(
                    open_position,
                    rankings=rankings,
                    reason=f"Rotate into top-pool entry-ready asset {next_entry['symbol']} with RSI 1h {next_entry.get('rsi_1h')}",
                )
                cash = self._cash_balance(starting_capital=starting_capital)
                if cash > 0 and float(next_entry.get("price") or 0) > 0:
                    self._open_new_position(next_entry, cash, action="BUY_NEXT_ENTRY_READY")
                else:
                    self._record_trade(action="CHECK_NO_CASH", symbol=next_entry["symbol"], price=float(next_entry.get("price") or 0), quantity=0.0, value=0.0, pnl=0.0, reason="No cash available for entry-ready rotation")
            else:
                price, price_source = self._price_with_source(open_position.symbol, rankings=rankings, fallback=open_position.entry_price)
                value, pnl = self._position_mark_to_market(position=open_position, mark_price=price)
                open_position.mark_price = price
                open_position.unrealized_pnl = pnl
                self._record_trade(
                    action="HOLD_NO_NEXT_ENTRY",
                    symbol=open_position.symbol,
                    price=price,
                    quantity=open_position.quantity,
                    value=value,
                    pnl=pnl,
                    reason=self._hold_reason(current_asset),
                    meta={"price_source": price_source, "pnl_basis": "mark_to_market"},
                )
            self.db.commit()
            return self._build_status(
                rankings=rankings,
                cadence_hours=cadence_hours,
                starting_capital=starting_capital,
                min_momentum_score=min_momentum_score,
            )

        best_asset = self._best_entry_ready_asset(rankings=rankings, min_momentum_score=min_momentum_score, exclude_symbols=set())
        if not best_asset:
            self._record_trade(
                action="CHECK_NO_ENTRY_READY_ASSET",
                symbol="NONE",
                price=0.0,
                quantity=0.0,
                value=0.0,
                pnl=0.0,
                reason=f"No top-pool momentum asset with valid 15m structure and RSI 1h between {self.ENTRY_RSI_MIN:g}-{self.ENTRY_RSI_MAX:g}",
            )
            self.db.commit()
            return self._build_status(
                rankings=rankings,
                cadence_hours=cadence_hours,
                starting_capital=starting_capital,
                min_momentum_score=min_momentum_score,
            )

        cash = self._cash_balance(starting_capital=starting_capital)
        if cash > 0 and float(best_asset.get("price") or 0) > 0:
            self._open_new_position(best_asset, cash, action="BUY_RSI_1H_ENTRY_READY")
        else:
            self._record_trade(action="CHECK_NO_CASH", symbol=best_asset["symbol"], price=float(best_asset.get("price") or 0), quantity=0.0, value=0.0, pnl=0.0, reason="No cash available for momentum entry")

        self.db.commit()
        return self._build_status(
            rankings=rankings,
            cadence_hours=cadence_hours,
            starting_capital=starting_capital,
            min_momentum_score=min_momentum_score,
        )


    def best_entry_ready_asset(self, *, rankings: list[dict[str, Any]], min_momentum_score: float, exclude_symbols: set[str]) -> dict[str, Any] | None:
        return self._best_entry_ready_asset(
            rankings=rankings,
            min_momentum_score=min_momentum_score,
            exclude_symbols=exclude_symbols,
        )

    def structure_broken(self, asset: dict[str, Any] | None) -> bool:
        return self._structure_broken(asset)

    def _rankings(self) -> list[dict[str, Any]]:
        return MomentumService(self.db).list_rankings(limit=300)

    def _build_status(self, *, rankings: list[dict[str, Any]], cadence_hours: int, starting_capital: float, min_momentum_score: float) -> dict[str, Any]:
        open_position = self._open_position()
        current_asset = self._asset_for(open_position.symbol, rankings=rankings) if open_position else None
        exclude_symbols = {open_position.symbol} if open_position else set()
        best_asset = self._best_entry_ready_asset(rankings=rankings, min_momentum_score=min_momentum_score, exclude_symbols=exclude_symbols)
        top_watch_asset = self._top_watch_asset(rankings=rankings, min_momentum_score=min_momentum_score, exclude_symbols=exclude_symbols)
        open_position_payload = self._position_payload(open_position, rankings=rankings, current_asset=current_asset) if open_position else None
        last_trade = self._last_check_trade()
        now = datetime.now(timezone.utc)
        last_check_at = self._as_utc(last_trade.created_at) if last_trade else None
        next_check_at = last_check_at + timedelta(hours=cadence_hours) if last_check_at else None
        due_now = next_check_at is None or now >= next_check_at
        cash = self._cash_balance(starting_capital=starting_capital)
        equity = cash + self._open_position_value(open_position, rankings=rankings)
        total_pnl = equity - starting_capital
        recommendation = self._recommendation(open_position=open_position, current_asset=current_asset, best_asset=best_asset, top_watch_asset=top_watch_asset, due_now=due_now)

        return {
            "strategy": self.STRATEGY,
            "mode": self.MODE,
            "cadence_hours": cadence_hours,
            "starting_capital": starting_capital,
            "cash": round(cash, 8),
            "equity": round(equity, 8),
            "total_pnl": round(total_pnl, 8),
            "total_pnl_pct": round((total_pnl / starting_capital) * 100, 4) if starting_capital else 0.0,
            "entry_rsi_timeframe": self.ENTRY_RSI_TIMEFRAME,
            "entry_rsi_min": self.ENTRY_RSI_MIN,
            "entry_rsi_max": self.ENTRY_RSI_MAX,
            "entry_pool_top_n": self.ENTRY_POOL_TOP_N,
            "entry_pool_min_leader_ratio": self.ENTRY_POOL_MIN_LEADER_RATIO,
            "open_position": open_position_payload,
            "best_asset": best_asset,
            "top_watch_asset": top_watch_asset,
            "last_check_at": last_check_at,
            "next_check_at": next_check_at,
            "due_now": due_now,
            "recommendation": recommendation,
            "trades": self._recent_trades(limit=50),
        }

    def _as_utc(self, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _asset_for(self, symbol: str, *, rankings: list[dict[str, Any]]) -> dict[str, Any] | None:
        for row in rankings:
            if row.get("symbol") == symbol:
                return row
        return None

    def _leader_score(self, rankings: list[dict[str, Any]]) -> float:
        if not rankings:
            return 0.0
        return float(rankings[0].get("momentum_score") or 0.0)

    def _in_entry_pool(self, asset: dict[str, Any], *, leader_score: float) -> bool:
        rank = int(asset.get("rank") or 999999)
        score = float(asset.get("momentum_score") or 0.0)
        if rank <= self.ENTRY_POOL_TOP_N:
            return True
        if leader_score > 0 and score >= leader_score * self.ENTRY_POOL_MIN_LEADER_RATIO:
            return True
        return False

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

    def _entry_status(self, asset: dict[str, Any] | None) -> str:
        if not asset:
            return "blocked_missing_asset"
        if not self._structure_valid(asset):
            return "blocked_structure_not_valid"
        rsi = asset.get(self.ENTRY_RSI_FIELD)
        if rsi is None:
            return "wait_rsi_1h_missing"
        rsi_value = float(rsi)
        if rsi_value < self.ENTRY_RSI_MIN:
            return "wait_recovery_rsi_1h_too_low"
        if rsi_value > self.ENTRY_RSI_MAX:
            return "wait_pullback_rsi_1h_too_high"
        return "ready"

    def _entry_ready(self, asset: dict[str, Any] | None) -> bool:
        return self._entry_status(asset) == "ready"

    def _decorate_entry(self, asset: dict[str, Any] | None) -> dict[str, Any] | None:
        if not asset:
            return None
        decorated = dict(asset)
        decorated["entry_status"] = self._entry_status(asset)
        decorated["entry_rsi_timeframe"] = self.ENTRY_RSI_TIMEFRAME
        decorated["entry_rsi_min"] = self.ENTRY_RSI_MIN
        decorated["entry_rsi_max"] = self.ENTRY_RSI_MAX
        decorated["in_entry_pool"] = True
        return decorated

    def _top_watch_asset(self, *, rankings: list[dict[str, Any]], min_momentum_score: float, exclude_symbols: set[str]) -> dict[str, Any] | None:
        leader_score = self._leader_score(rankings)
        for row in rankings:
            if row.get("symbol") in exclude_symbols:
                continue
            if float(row.get("price") or 0) <= 0:
                continue
            if float(row.get("momentum_score") or 0) <= min_momentum_score:
                continue
            if not self._in_entry_pool(row, leader_score=leader_score):
                continue
            if not self._structure_valid(row):
                continue
            return self._decorate_entry(row)
        return None

    def _best_entry_ready_asset(self, *, rankings: list[dict[str, Any]], min_momentum_score: float, exclude_symbols: set[str]) -> dict[str, Any] | None:
        leader_score = self._leader_score(rankings)
        for row in rankings:
            if row.get("symbol") in exclude_symbols:
                continue
            if float(row.get("price") or 0) <= 0:
                continue
            if float(row.get("momentum_score") or 0) <= min_momentum_score:
                continue
            if not self._in_entry_pool(row, leader_score=leader_score):
                continue
            if not self._entry_ready(row):
                continue
            return self._decorate_entry(row)
        return None

    def _hold_reason(self, asset: dict[str, Any] | None) -> str:
        if not asset:
            return "Current asset not found in ranking, holding until explicit structure break or new entry-ready asset."
        return f"Hold: 15m structure still holding and no top-pool RSI 1h entry is ready. Current structure: {asset.get('structure_15m_status')} / {asset.get('structure_reason')}"

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
        sell_value, buy_value = self.db.execute(
            select(
                func.coalesce(
                    func.sum(case((MomentumEngineTrade.action.like("SELL%"), MomentumEngineTrade.value), else_=0.0)),
                    0.0,
                ),
                func.coalesce(
                    func.sum(case((MomentumEngineTrade.action.like("BUY%"), MomentumEngineTrade.value), else_=0.0)),
                    0.0,
                ),
            ).where(MomentumEngineTrade.strategy == self.STRATEGY)
        ).one()
        return starting_capital + float(sell_value or 0.0) - float(buy_value or 0.0)

    def _position_payload(self, position: MomentumEnginePosition, *, rankings: list[dict[str, Any]], current_asset: dict[str, Any] | None) -> dict[str, Any]:
        mark_price, mark_price_source = self._price_with_source(position.symbol, rankings=rankings, fallback=position.entry_price)
        _, unrealized_pnl = self._position_mark_to_market(position=position, mark_price=mark_price)
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
            "mark_price_source": mark_price_source,
            "unrealized_pnl": unrealized_pnl,
            "opened_at": position.opened_at,
            "closed_at": position.closed_at,
            "structure_15m_status": current_asset.get("structure_15m_status") if current_asset else "missing",
            "structure_15m_bias": current_asset.get("structure_15m_bias") if current_asset else "missing",
            "structure_reason": current_asset.get("structure_reason") if current_asset else "asset_missing_from_ranking",
            "structure_broken": self._structure_broken(current_asset),
            "rsi_1h": current_asset.get("rsi_1h") if current_asset else None,
            "entry_status": self._entry_status(current_asset) if current_asset else "blocked_missing_asset",
        }

    def _open_position_value(self, position: MomentumEnginePosition | None, *, rankings: list[dict[str, Any]]) -> float:
        if not position:
            return 0.0
        mark_price = self._price_for(position.symbol, rankings=rankings, fallback=position.entry_price)
        value, _ = self._position_mark_to_market(position=position, mark_price=mark_price)
        return value

    def _position_mark_to_market(self, *, position: MomentumEnginePosition, mark_price: float) -> tuple[float, float]:
        value = float(position.quantity or 0) * float(mark_price or 0)
        pnl = value - float(position.entry_value or 0)
        return value, pnl

    def _latest_market_price(self, symbol: str) -> tuple[float, str] | None:
        normalized = symbol.upper()
        stmt = (
            select(MarketCandle.close, MarketCandle.interval)
            .where(MarketCandle.symbol == normalized, MarketCandle.close > 0)
            .order_by(MarketCandle.close_time.desc(), MarketCandle.open_time.desc())
            .limit(1)
        )
        row = self.db.execute(stmt).first()
        if not row:
            return None
        price, interval = row
        return float(price), f"market_candle:{interval}"

    def _price_with_source(self, symbol: str, *, rankings: list[dict[str, Any]], fallback: float) -> tuple[float, str]:
        # Momentum PnL must be marked with the freshest known market close, not
        # the entry-price fallback or an old ranking snapshot. Otherwise HOLD or
        # rotation events can show artificial 0-PnL while the market moved.
        market_price = self._latest_market_price(symbol)
        if market_price is not None:
            return market_price
        for row in rankings:
            if row.get("symbol") == symbol and row.get("price"):
                return float(row["price"]), "ranking_snapshot"
        return float(fallback or 0), "fallback"

    def _price_for(self, symbol: str, *, rankings: list[dict[str, Any]], fallback: float) -> float:
        price, _ = self._price_with_source(symbol, rankings=rankings, fallback=fallback)
        return price

    def _open_new_position(self, asset: dict[str, Any], cash: float, *, action: str) -> MomentumEnginePosition:
        price, price_source = self._price_with_source(asset["symbol"], rankings=[asset], fallback=float(asset["price"]))
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
                "rsi_1h": asset.get("rsi_1h"),
                "entry_status": asset.get("entry_status"),
                "entry_rsi_timeframe": self.ENTRY_RSI_TIMEFRAME,
            },
            opened_at=now,
        )
        self.db.add(position)
        self._record_trade(
            action=action,
            symbol=asset["symbol"],
            price=price,
            quantity=quantity,
            value=cash,
            pnl=0.0,
            reason=f"Top-pool momentum rank #{asset.get('rank')} with valid 15m structure and RSI 1h {asset.get('rsi_1h')} in {self.ENTRY_RSI_MIN:g}-{self.ENTRY_RSI_MAX:g}",
            meta={"price_source": price_source, "pnl_basis": "entry"},
        )
        return position

    def _close_position(self, position: MomentumEnginePosition, *, rankings: list[dict[str, Any]], reason: str) -> None:
        price, price_source = self._price_with_source(position.symbol, rankings=rankings, fallback=position.entry_price)
        value, pnl = self._position_mark_to_market(position=position, mark_price=price)
        position.status = "closed"
        position.mark_price = price
        position.unrealized_pnl = pnl
        position.closed_at = datetime.now(timezone.utc)
        self._record_trade(
            action="SELL_ROTATE_OR_STRUCTURE_BREAK",
            symbol=position.symbol,
            price=price,
            quantity=position.quantity,
            value=value,
            pnl=pnl,
            reason=reason,
            meta={"price_source": price_source, "pnl_basis": "realized"},
        )

    def _record_trade(self, *, action: str, symbol: str, price: float, quantity: float, value: float, pnl: float, reason: str, meta: dict[str, Any] | None = None) -> MomentumEngineTrade:
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
            meta=meta or {},
            created_at=datetime.now(timezone.utc),
        )
        self.db.add(trade)
        return trade

    def _recommendation(self, *, open_position: MomentumEnginePosition | None, current_asset: dict[str, Any] | None, best_asset: dict[str, Any] | None, top_watch_asset: dict[str, Any] | None, due_now: bool) -> str:
        if open_position and self._structure_broken(current_asset):
            target = best_asset.get("symbol") if best_asset else "cash"
            return f"Sell {open_position.symbol}: 15m structure broken. Rotate to {target}."
        if open_position and best_asset:
            return f"Rotate from {open_position.symbol} to {best_asset['symbol']} — RSI 1h entry ready ({best_asset.get('rsi_1h')})."
        if open_position:
            return f"Hold {open_position.symbol}: no top-pool RSI 1h entry is ready and 15m structure has not broken."
        if best_asset:
            return f"Buy {best_asset['symbol']} — RSI 1h entry ready ({best_asset.get('rsi_1h')})." if due_now else "Wait until next scheduled momentum check."
        if top_watch_asset:
            return f"Wait on {top_watch_asset['symbol']}: {top_watch_asset.get('entry_status')} (RSI 1h={top_watch_asset.get('rsi_1h')})."
        return f"No top-pool momentum asset with valid 15m structure and RSI 1h between {self.ENTRY_RSI_MIN:g}-{self.ENTRY_RSI_MAX:g}."

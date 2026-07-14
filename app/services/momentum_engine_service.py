from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from fastapi.encoders import jsonable_encoder
from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from app.models.market_candle import MarketCandle
from app.models.momentum_engine import MomentumEnginePosition, MomentumEngineTrade
from app.models.momentum_engine_current_decision import MomentumEngineCurrentDecision
from app.services.momentum_service import MomentumService


class MomentumEngineService:
    """Dedicated backend paper engine for momentum rotation.

    Strategy v3:
    - Exit risk is still managed by bearish 15m structure.
    - Entry selection follows the momentum ranking order strictly.
    - The engine targets the highest-ranked available asset with positive price,
      sufficient momentum score, and intact 15m support/structure.
    - A current position is sold only if 15m structure breaks or a better-ranked
      eligible momentum asset is available.
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

    def decision(self, *, cadence_hours: int = 4, starting_capital: float = 1000.0, min_momentum_score: float = 0.0) -> dict[str, Any]:
        rankings = self._rankings()
        status = self._build_status(
            rankings=rankings,
            cadence_hours=cadence_hours,
            starting_capital=starting_capital,
            min_momentum_score=min_momentum_score,
        )
        return self._decision_from_status(status)

    def current_decision(self) -> dict[str, Any]:
        """Return the latest persisted momentum-engine decision payload."""
        current = self.db.get(MomentumEngineCurrentDecision, 1)
        if current and isinstance(current.payload_json, dict) and current.payload_json:
            return current.payload_json
        return self._empty_current_decision()

    def _empty_current_decision(self) -> dict[str, Any]:
        """Return a stable executor-compatible fallback when no current snapshot exists."""
        message = "No persisted momentum decision available yet."
        return {
            "strategy": self.STRATEGY,
            "mode": self.MODE,
            "cadence_hours": 4,
            "starting_capital": 1000.0,
            "cash": 0.0,
            "equity": 0.0,
            "total_pnl": 0.0,
            "total_pnl_pct": 0.0,
            "action": "WAIT",
            "decision_action": "WAIT",
            "symbol": None,
            "target_symbol": None,
            "buy_symbol": None,
            "sell_symbol": None,
            "should_trade": False,
            "status": "no_persisted_decision",
            "recommendation": message,
            "reason": message,
            "due_now": False,
            "open_position": None,
            "best_asset": None,
            "top_watch_asset": None,
            "last_check_at": None,
            "next_check_at": None,
            "source": "persisted_current_decision",
        }

    def run_once(self, *, force: bool = False, cadence_hours: int = 4, starting_capital: float = 1000.0, min_momentum_score: float = 0.0) -> dict[str, Any]:
        rankings = self._rankings()
        before = self._build_status(
            rankings=rankings,
            cadence_hours=cadence_hours,
            starting_capital=starting_capital,
            min_momentum_score=min_momentum_score,
        )
        if not force and not before["due_now"]:
            decision = self._decision_from_status(before)
            self._save_current_decision(decision)
            self.db.commit()
            return before

        open_position = self._open_position()
        current_asset = self._asset_for(open_position.symbol, rankings=rankings) if open_position else None
        current_broken = self._structure_broken(current_asset) if current_asset else False
        if open_position:
            if current_broken:
                self._close_position(
                    open_position,
                    rankings=rankings,
                    reason="15m support broken on current asset; rotating to next highest momentum asset with valid 15m support",
                )
                replacement_asset = self._best_ranked_asset_with_valid_structure(
                    rankings=rankings,
                    min_momentum_score=min_momentum_score,
                    exclude_symbols={open_position.symbol},
                )
                if replacement_asset:
                    self.db.flush()
                    cash = self._cash_balance(starting_capital=starting_capital)
                    if cash > 0 and float(replacement_asset.get("price") or 0) > 0:
                        self._open_new_position(
                            replacement_asset,
                            cash,
                            action="BUY_NEXT_VALID_MOMENTUM_AFTER_15M_BREAK",
                            reason="15m support broken on current asset; rotating to next highest momentum asset with valid 15m support",
                        )
                    else:
                        self._record_trade(action="CHECK_NO_CASH", symbol=replacement_asset["symbol"], price=float(replacement_asset.get("price") or 0), quantity=0.0, value=0.0, pnl=0.0, reason="No cash available after 15m support break on current asset")
                else:
                    self._record_trade(action="STAY_CASH_AFTER_STRUCTURE_BREAK", symbol="NONE", price=0.0, quantity=0.0, value=0.0, pnl=0.0, reason="15m support broken on current asset; no ranked momentum asset has valid 15m support")
            else:
                candidate = self._best_ranked_asset_with_valid_structure(
                    rankings=rankings,
                    min_momentum_score=min_momentum_score,
                    exclude_symbols={open_position.symbol},
                )
                current_rank = current_asset.get("rank") if current_asset else None
                candidate_rank = candidate.get("rank") if candidate else None
                replacement_asset = (
                    candidate
                    if candidate is not None
                    and self._is_better_rank(candidate_rank=candidate_rank, current_rank=current_rank)
                    else None
                )
                if replacement_asset:
                    self._close_position(
                        open_position,
                        rankings=rankings,
                        reason=f"Rotate into better-ranked eligible momentum asset {replacement_asset['symbol']}",
                    )
                    self.db.flush()
                    cash = self._cash_balance(starting_capital=starting_capital)
                    if cash > 0 and float(replacement_asset.get("price") or 0) > 0:
                        self._open_new_position(replacement_asset, cash, action="BUY_NEXT_ENTRY_READY")
                    else:
                        self._record_trade(action="CHECK_NO_CASH", symbol=replacement_asset["symbol"], price=float(replacement_asset.get("price") or 0), quantity=0.0, value=0.0, pnl=0.0, reason="No cash available for ranked momentum rotation")
                else:
                    price, price_source = self._price_with_source(open_position.symbol, rankings=rankings, fallback=open_position.entry_price)
                    value, pnl = self._position_mark_to_market(position=open_position, mark_price=price)
                    open_position.mark_price = price
                    open_position.unrealized_pnl = pnl
                    self._record_trade(
                        action="HOLD_NO_BETTER_VALID_MOMENTUM",
                        symbol=open_position.symbol,
                        price=price,
                        quantity=open_position.quantity,
                        value=value,
                        pnl=pnl,
                        reason=self._hold_reason(current_asset),
                        meta={"price_source": price_source, "pnl_basis": "mark_to_market"},
                    )
            self.db.flush()
            status = self._build_status(
                rankings=rankings,
                cadence_hours=cadence_hours,
                starting_capital=starting_capital,
                min_momentum_score=min_momentum_score,
            )
            decision = self._decision_from_status(status)
            self._save_current_decision(decision)
            self.db.commit()
            return status

        best_asset = self._best_ranked_asset_with_valid_structure(rankings=rankings, min_momentum_score=min_momentum_score, exclude_symbols=set())
        if not best_asset:
            self._record_trade(
                action="CHECK_NO_RANKED_ASSET",
                symbol="NONE",
                price=0.0,
                quantity=0.0,
                value=0.0,
                pnl=0.0,
                reason="No ranked momentum asset has positive price, sufficient score, and valid 15m structure",
            )
            self.db.flush()
            status = self._build_status(
                rankings=rankings,
                cadence_hours=cadence_hours,
                starting_capital=starting_capital,
                min_momentum_score=min_momentum_score,
            )
            decision = self._decision_from_status(status)
            self._save_current_decision(decision)
            self.db.commit()
            return status

        cash = self._cash_balance(starting_capital=starting_capital)
        if cash > 0 and float(best_asset.get("price") or 0) > 0:
            self._open_new_position(best_asset, cash, action="BUY_MOMENTUM_ENTRY_READY")
        else:
            self._record_trade(action="CHECK_NO_CASH", symbol=best_asset["symbol"], price=float(best_asset.get("price") or 0), quantity=0.0, value=0.0, pnl=0.0, reason="No cash available for momentum entry")

        self.db.flush()
        status = self._build_status(
            rankings=rankings,
            cadence_hours=cadence_hours,
            starting_capital=starting_capital,
            min_momentum_score=min_momentum_score,
        )
        decision = self._decision_from_status(status)
        self._save_current_decision(decision)
        self.db.commit()
        return status

    def _rankings(self) -> list[dict[str, Any]]:
        return MomentumService(self.db).list_rankings(limit=300)

    def _build_status(self, *, rankings: list[dict[str, Any]], cadence_hours: int, starting_capital: float, min_momentum_score: float) -> dict[str, Any]:
        open_position = self._open_position()
        current_asset = self._asset_for(open_position.symbol, rankings=rankings) if open_position else None
        exclude_symbols = {open_position.symbol} if open_position else set()
        current_broken = self._structure_broken(current_asset) if open_position else False
        if open_position and not current_broken:
            best_asset = self._better_ranked_asset_with_valid_structure(
                current_asset=current_asset,
                rankings=rankings,
                min_momentum_score=min_momentum_score,
                exclude_symbols=exclude_symbols,
            )
        else:
            best_asset = self._best_ranked_asset_with_valid_structure(rankings=rankings, min_momentum_score=min_momentum_score, exclude_symbols=exclude_symbols)
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
            "selection_method": "momentum_rank_with_valid_15m_structure",
            "selection_min_momentum_score": min_momentum_score,
            "open_position": open_position_payload,
            "best_asset": best_asset,
            "top_watch_asset": top_watch_asset,
            "last_check_at": last_check_at,
            "next_check_at": next_check_at,
            "due_now": due_now,
            "recommendation": recommendation,
            "trades": self._recent_trades(limit=50),
        }


    def _decision_from_status(self, status: dict[str, Any]) -> dict[str, Any]:
        open_position = status.get("open_position")
        best_asset = status.get("best_asset")
        top_watch_asset = status.get("top_watch_asset")
        due_now = bool(status.get("due_now"))
        recommendation = str(status.get("recommendation") or "")

        action = "no_entry"
        symbol = None
        target_symbol = None
        reason = recommendation

        if open_position:
            symbol = open_position.get("symbol")
            if open_position.get("structure_broken"):
                if best_asset:
                    action = "rotate"
                    target_symbol = best_asset.get("symbol")
                    reason = f"15m structure is broken on {symbol}; rotate to best-ranked eligible momentum asset {target_symbol}."
                else:
                    action = "sell"
                    target_symbol = "CASH"
                    reason = f"15m structure is broken on {symbol}; sell and stay in cash because no ranked replacement has valid 15m structure."
            elif best_asset:
                action = "rotate" if due_now else "wait"
                target_symbol = best_asset.get("symbol")
                reason = (
                    f"Replacement asset {target_symbol} is a better-ranked eligible momentum asset now."
                    if due_now
                    else f"Replacement asset {target_symbol} is a better-ranked eligible momentum asset, but the next scheduled momentum check is not due yet."
                )
            else:
                action = "hold"
                target_symbol = symbol
                reason = "Current position structure is holding and no better-ranked momentum asset has valid 15m structure."
        elif best_asset:
            action = "buy" if due_now else "wait"
            symbol = best_asset.get("symbol")
            target_symbol = symbol
            reason = (
                f"Momentum asset {symbol} is the best-ranked eligible asset and the engine check is due now."
                if due_now
                else f"Momentum asset {symbol} is the best-ranked eligible asset, but the next scheduled momentum check is not due yet."
            )
        elif top_watch_asset:
            action = "wait"
            symbol = top_watch_asset.get("symbol")
            target_symbol = symbol
            reason = f"Best watched asset is not eligible yet: {top_watch_asset.get('entry_status')}."
        else:
            reason = recommendation or "No eligible ranked momentum asset has valid 15m structure."

        return {
            **status,
            "action": action,
            "symbol": symbol,
            "target_symbol": target_symbol,
            "reason": reason,
        }

    def _save_current_decision(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Persist the current executor-compatible decision snapshot and return it."""
        now = datetime.now(timezone.utc)
        decision = jsonable_encoder(dict(payload))
        action = str(decision.get("action") or "").lower() or None
        target_symbol = decision.get("target_symbol")
        symbol = decision.get("symbol")
        due_now = bool(decision.get("due_now"))

        decision.setdefault("decision_action", action)
        if "buy_symbol" not in decision:
            decision["buy_symbol"] = target_symbol if action in {"buy", "rotate"} else None
        if "sell_symbol" not in decision:
            decision["sell_symbol"] = symbol if action in {"sell", "rotate"} else None
        if "should_trade" not in decision:
            decision["should_trade"] = bool(due_now and action in {"buy", "sell", "rotate"})
        if "status" not in decision:
            decision["status"] = "trade_ready" if decision["should_trade"] else "no_trade"
        decision.setdefault("produced_at", now.isoformat())
        decision.setdefault("source", "persisted_current_decision")

        row = self.db.get(MomentumEngineCurrentDecision, 1)
        if row is None:
            row = MomentumEngineCurrentDecision(id=1)
            self.db.add(row)

        produced_at = decision.get("produced_at")
        if isinstance(produced_at, str):
            parsed_produced_at = datetime.fromisoformat(produced_at.replace("Z", "+00:00"))
        elif isinstance(produced_at, datetime):
            parsed_produced_at = produced_at
        else:
            parsed_produced_at = now

        row.decision_id = str(decision.get("decision_id") or f"momentum-current-{uuid4().hex}")
        row.strategy = decision.get("strategy")
        row.action = action
        row.decision_action = decision.get("decision_action")
        row.symbol = symbol
        row.target_symbol = target_symbol
        row.buy_symbol = decision.get("buy_symbol")
        row.sell_symbol = decision.get("sell_symbol")
        row.should_trade = bool(decision.get("should_trade"))
        row.status = decision.get("status")
        row.reason = decision.get("reason")
        row.due_now = due_now
        row.payload_json = decision
        row.produced_at = parsed_produced_at
        row.updated_at = now
        self.db.flush()
        return decision

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
        if float(asset.get("price") or 0) <= 0:
            return "blocked_price_not_positive"
        if not self._structure_valid(asset):
            return "blocked_structure_not_valid"
        return "ready"

    def _decorate_entry(self, asset: dict[str, Any] | None) -> dict[str, Any] | None:
        if not asset:
            return None
        decorated = dict(asset)
        entry_status = self._entry_status(asset)
        decorated["entry_status"] = entry_status
        decorated["selection_status"] = entry_status
        decorated["selection_method"] = "momentum_rank_with_valid_15m_structure"
        return decorated

    def _top_watch_asset(self, *, rankings: list[dict[str, Any]], min_momentum_score: float, exclude_symbols: set[str] | None = None) -> dict[str, Any] | None:
        excluded = exclude_symbols or set()
        for row in rankings:
            if row.get("symbol") in excluded:
                continue
            if float(row.get("price") or 0) <= 0:
                continue
            if float(row.get("momentum_score") or 0) <= min_momentum_score:
                continue
            return self._decorate_entry(row)
        return None

    def _best_ranked_asset_with_valid_structure(self, *, rankings: list[dict[str, Any]], min_momentum_score: float, exclude_symbols: set[str] | None = None) -> dict[str, Any] | None:
        excluded = exclude_symbols or set()
        for row in rankings:
            if row.get("symbol") in excluded:
                continue
            if float(row.get("price") or 0) <= 0:
                continue
            if float(row.get("momentum_score") or 0) <= min_momentum_score:
                continue
            if not self._structure_valid(row):
                continue
            return self._decorate_entry(row)
        return None

    def _is_better_rank(self, *, candidate_rank: Any, current_rank: Any) -> bool:
        if candidate_rank is None or current_rank is None:
            return False
        try:
            return int(candidate_rank) < int(current_rank)
        except (TypeError, ValueError):
            return False

    def _better_ranked_asset_with_valid_structure(self, *, current_asset: dict[str, Any] | None, rankings: list[dict[str, Any]], min_momentum_score: float, exclude_symbols: set[str] | None = None) -> dict[str, Any] | None:
        if not current_asset:
            return self._best_ranked_asset_with_valid_structure(
                rankings=rankings,
                min_momentum_score=min_momentum_score,
                exclude_symbols=exclude_symbols,
            )
        current_rank = current_asset.get("rank")
        excluded = exclude_symbols or set()
        for row in rankings:
            if row.get("symbol") in excluded:
                continue
            row_rank = row.get("rank")
            if not self._is_better_rank(candidate_rank=row_rank, current_rank=current_rank):
                continue
            if float(row.get("price") or 0) <= 0:
                continue
            if float(row.get("momentum_score") or 0) <= min_momentum_score:
                continue
            if not self._structure_valid(row):
                continue
            return self._decorate_entry(row)
        return None

    def _hold_reason(self, asset: dict[str, Any] | None) -> str:
        if not asset:
            return "Current asset not found in ranking, holding until explicit structure break or new ranked momentum asset."
        return f"Hold: 15m structure still holding and no alternate ranked momentum asset is eligible. Current structure: {asset.get('structure_15m_status')} / {asset.get('structure_reason')}"

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
        realized_pnl = self.db.scalar(
            select(
                func.coalesce(
                    func.sum(case((MomentumEngineTrade.action.like("SELL%"), MomentumEngineTrade.pnl), else_=0.0)),
                    0.0,
                )
            ).where(MomentumEngineTrade.strategy == self.STRATEGY)
        )
        open_entry_value = self.db.scalar(
            select(func.coalesce(func.sum(MomentumEnginePosition.entry_value), 0.0)).where(
                MomentumEnginePosition.strategy == self.STRATEGY,
                MomentumEnginePosition.status == "open",
            )
        )
        paper_cash = starting_capital + float(realized_pnl or 0.0) - float(open_entry_value or 0.0)
        return max(0.0, paper_cash)

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

    def _open_new_position(self, asset: dict[str, Any], cash: float, *, action: str, reason: str | None = None) -> MomentumEnginePosition:
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
                "selection_method": asset.get("selection_method"),
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
            reason=reason or f"Momentum rank #{asset.get('rank')} with positive price, sufficient momentum score, and valid 15m structure",
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
            return f"Rotate from {open_position.symbol} to {best_asset['symbol']} — better-ranked eligible momentum asset."
        if open_position:
            return f"Hold {open_position.symbol}: no better-ranked momentum asset is eligible and 15m structure has not broken."
        if best_asset:
            return f"Buy {best_asset['symbol']} — best-ranked eligible momentum asset." if due_now else "Wait until next scheduled momentum check."
        if top_watch_asset:
            return f"Wait on {top_watch_asset['symbol']}: {top_watch_asset.get('entry_status')}."
        return "No ranked momentum asset has positive price, sufficient score, and valid 15m structure."

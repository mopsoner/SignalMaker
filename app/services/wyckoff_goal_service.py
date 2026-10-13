"""Monitor exchange-confirmed Wyckoff positions; GET endpoints never submit orders."""
from datetime import datetime, timezone
import math

from sqlalchemy import select, update

from app.core.config import settings
from app.models.position import Position
from app.services.execution.kraken_execution_service import KrakenExecutionService


class WyckoffGoalService:
    def __init__(self, db, execution=None):
        self.db = db
        self.execution = execution

    def rows(self):
        return [row for row in self.db.scalars(select(Position).where(
            Position.status.in_(["open", "closing"])
        )).all() if (row.meta or {}).get("execution_mode") == "live"]

    def summary(self):
        rows = self.rows()
        verified = all((row.meta or {}).get("goal_verified_at") for row in rows)
        total = sum(row.unrealized_pnl or 0 for row in rows)
        return {"targetUsd": settings.wyckoff_live_goal_usd,
                "profitUsd": total if verified else None, "positionCount": len(rows),
                "verified": verified, "closing": any(row.status == "closing" or
                (row.meta or {}).get("goal_triggered") for row in rows),
                "lastCheckedAt": min(((row.meta or {}).get("goal_verified_at", "") for row in rows), default=None),
                "errors": [(row.meta or {}).get("goal_error") for row in rows if (row.meta or {}).get("goal_error")]}

    def _meta(self, row, **values):
        row.meta = {**(row.meta or {}), **values}
        self.db.commit()

    def _scan(self, row, margin_positions):
        ex = self.execution
        mode = (row.meta or {}).get("kraken_mode", settings.wyckoff_live_mode)
        quote = ex.rules.quote_asset(row.symbol)
        fx = 1.0 if quote in {"USD", "ZUSD"} else float(ex.client.current_price(f"{quote}USD"))
        if mode == "margin":
            matches = [p for p in margin_positions.values() if p.get("ordertxid") == row.entry_order_id]
            quantity = sum(float(p["vol"]) - float(p.get("vol_closed", 0)) for p in matches)
        else:
            entry = ex.get_order(row.symbol, row.entry_order_id, mode=mode)
            if entry.get("status") not in {"filled", "open", "canceled", "expired"}:
                raise ValueError("Kraken entry order could not be verified")
            quantity = float(entry["executed_quantity"])
            if row.take_profit_order_id:
                tp = ex.get_order(row.symbol, row.take_profit_order_id, mode=mode)
                row.take_profit_order_status = tp["status"]
                quantity -= float(tp.get("executed_quantity", 0))
            quantity -= float((row.meta or {}).get("goal_closed_quantity", 0))
        if quantity <= 0:
            row.status = "closed"
            row.closed_at = datetime.now(timezone.utc)
            self.db.commit()
            return
        mark = float(ex.client.current_price(row.symbol))
        entry_price = float(row.entry_price)
        short = row.side.lower() in {"short", "sell", "bear", "bear_watch"}
        pnl = (entry_price - mark if short else mark - entry_price) * quantity * fx
        if not all(math.isfinite(v) for v in (mark, entry_price, quantity, pnl)) or min(mark, entry_price) <= 0:
            raise ValueError("Invalid Kraken position prices or quantity")
        row.quantity, row.mark_price, row.unrealized_pnl = quantity, mark, pnl
        self._meta(row, goal_verified_at=datetime.now(timezone.utc).isoformat(), goal_error=None)

    def _close(self, row):
        ex = self.execution
        mode = (row.meta or {}).get("kraken_mode", settings.wyckoff_live_mode)
        close_id = (row.meta or {}).get("goal_close_order_id")
        if close_id:
            order = ex.get_order(row.symbol, close_id, mode=mode)
            if order["status"] == "filled":
                row.status = "closed"
                row.closed_at = datetime.now(timezone.utc)
                row.mark_price = float(order["average_price"])
                self._meta(row, goal_error=None)
            elif order["status"] in {"canceled", "expired"}:
                # Refresh remaining exposure on next tick before submitting a replacement.
                row.status = "open"
                self._meta(row, goal_close_order_id=None, goal_closed_quantity=
                           float((row.meta or {}).get("goal_closed_quantity", 0)) + float(order.get("executed_quantity", 0)))
            return
        if row.status == "closing":
            # A lost response after submission must not cause a duplicate spot sale.
            return
        if mode == "margin":
            # Account-wide goal: cancel every exit on this pair, including externally placed TP/SL.
            pair = ex.client.pair_info(row.symbol)["pair_key"]
            exit_side = "buy" if row.side.lower() in {"short", "sell", "bear", "bear_watch"} else "sell"
            for order in ex.client.open_orders():
                descr = order.get("descr", {})
                if (ex.client.pair_info(descr.get("pair", ""))["pair_key"] == pair
                        and descr.get("type") == exit_side and descr.get("ordertype") != "market"):
                    ex.cancel_order(row.symbol, order["order_id"], mode=mode)
            row.take_profit_order_status = "canceled"
            self.db.commit()
        elif row.take_profit_order_id:
            ex.cancel_order(row.symbol, row.take_profit_order_id, mode=mode)
            row.take_profit_order_status = "canceled"
            self.db.commit()
        # Re-query after cancellation: the TP may have filled during the scan.
        self._scan(row, ex.client.open_margin_positions() if mode == "margin" else {})
        if row.status == "closed":
            return
        claimed = self.db.execute(update(Position).where(
            Position.position_id == row.position_id, Position.status == "open"
        ).values(status="closing"))
        self.db.commit()
        if claimed.rowcount != 1:
            return
        short = row.side.lower() in {"short", "sell", "bear", "bear_watch"}
        entry = ex.get_order(row.symbol, row.entry_order_id, mode=mode)
        order = ex.close_market(row.symbol, "buy" if short else "sell", row.quantity,
                                mode=mode, leverage=entry.get("leverage"))
        self._meta(row, goal_close_order_id=order["order_id"], goal_error=None)

    def run(self):
        if not settings.wyckoff_live_enabled or not settings.kraken_execution_enabled or settings.kraken_dry_run:
            return {"enabled": False, "goal": self.summary()}
        self.execution = self.execution or KrakenExecutionService(self.db)
        margin = self.execution.client.open_margin_positions()
        rows = self.rows()
        known = {r.entry_order_id for r in rows}
        for position_id, item in margin.items():
            if item.get("ordertxid") in known:
                continue
            quantity = float(item["vol"]) - float(item.get("vol_closed", 0))
            if quantity <= 0:
                continue
            local_id = f"kraken-{position_id}"
            row = self.db.get(Position, local_id)
            if row is None:
                row = Position(position_id=local_id, symbol=item["pair"],
                               side="long" if item["type"] == "buy" else "short",
                               quantity=quantity, entry_price=float(item["cost"]) / float(item["vol"]),
                               entry_order_id=item["ordertxid"], status="open",
                               meta={"execution_mode": "live", "kraken_mode": "margin", "goal_imported": True})
                self.db.add(row)
                self.db.commit()
        rows = self.rows()
        if not rows:
            return {"enabled": True, "goal": self.summary()}
        complete = True
        for row in rows:
            try:
                if row.status != "closing":
                    self._scan(row, margin)
            except Exception as exc:
                complete = False
                self._meta(row, goal_verified_at=None, goal_error=str(exc))
        rows = self.rows()
        triggered = any((r.meta or {}).get("goal_triggered") or r.status == "closing" for r in rows)
        if complete and sum(r.unrealized_pnl or 0 for r in rows) >= settings.wyckoff_live_goal_usd:
            triggered = True
            for row in rows:
                self._meta(row, goal_triggered=True)
        if triggered:
            for row in rows:
                try:
                    # Never sell from a failed/stale scan; pending close orders can still be checked.
                    if row.status == "closing" or (row.meta or {}).get("goal_verified_at"):
                        self._close(row)
                except Exception as exc:
                    self._meta(row, goal_error=str(exc))
        return {"enabled": True, "goal": self.summary(), "blockEntries": triggered or not complete}

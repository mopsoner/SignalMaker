from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.models.asset_state import AssetStateCurrent
from app.models.position import Position


SHORT_SIDES = {"short", "sell", "bear", "bear_watch"}
LONG_SIDES = {"long", "buy", "bull", "bull_watch"}


def _normalized_side(side: str | None) -> str:
    value = (side or "").lower()
    if value in SHORT_SIDES:
        return "short"
    if value in LONG_SIDES:
        return "long"
    return value


def _is_pnl_side(side: str | None) -> bool:
    return _normalized_side(side) in {"long", "short"}


def _safe_div(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def _position_pnl(*, side: str | None, entry_price: float | None, mark_price: float | None, quantity: float | None) -> float | None:
    if entry_price is None or mark_price is None or quantity is None:
        return None
    entry = float(entry_price)
    mark = float(mark_price)
    qty = float(quantity)
    if _normalized_side(side) == "short":
        return (entry - mark) * qty
    return (mark - entry) * qty


def _is_valid_stop(row: Position) -> bool:
    if row.entry_price is None or row.stop_price is None or not _is_pnl_side(row.side):
        return False
    entry = float(row.entry_price)
    stop = float(row.stop_price)
    return stop > entry if _normalized_side(row.side) == "short" else stop < entry


def _is_valid_target(row: Position) -> bool:
    if row.entry_price is None or row.target_price is None or not _is_pnl_side(row.side):
        return False
    entry = float(row.entry_price)
    target = float(row.target_price)
    return target < entry if _normalized_side(row.side) == "short" else target > entry


def _has_triggered_stop(row: Position) -> bool:
    if row.mark_price is None or not _is_valid_stop(row):
        return False
    mark = float(row.mark_price)
    stop = float(row.stop_price)
    return mark >= stop if _normalized_side(row.side) == "short" else mark <= stop


def _has_triggered_target(row: Position) -> bool:
    if row.mark_price is None or not _is_valid_target(row):
        return False
    mark = float(row.mark_price)
    target = float(row.target_price)
    return mark <= target if _normalized_side(row.side) == "short" else mark >= target


def _effective_pnl_price(row: Position) -> float | None:
    if row.mark_price is None:
        return None
    if _has_triggered_stop(row) and row.stop_price is not None:
        return float(row.stop_price)
    return float(row.mark_price)


def _effective_sl_tp_pnl_price(row: Position) -> float | None:
    if row.mark_price is None:
        return None
    if _has_triggered_stop(row) and row.stop_price is not None:
        return float(row.stop_price)
    if _has_triggered_target(row) and row.target_price is not None:
        return float(row.target_price)
    return float(row.mark_price)


def _pnl_from_price(row: Position, price: float | None) -> float | None:
    if row.entry_price is None or price is None or row.quantity is None or not _is_pnl_side(row.side):
        return None
    entry = float(row.entry_price)
    qty = float(row.quantity)
    if _normalized_side(row.side) == "short":
        return (entry - price) * qty
    return (price - entry) * qty


def _pnl_pct_from_price(row: Position, price: float | None) -> float | None:
    if row.entry_price is None or price is None or not _is_pnl_side(row.side):
        return None
    entry = float(row.entry_price)
    if entry == 0:
        return None
    if _normalized_side(row.side) == "short":
        return ((entry - price) / entry) * 100
    return ((price - entry) / entry) * 100


def _empty_summary() -> dict:
    return {
        "totalPnlPercent": 0.0,
        "averagePnlPercent": 0.0,
        "totalPnlValue": 0.0,
        "count": 0,
        "stoppedCount": 0,
        "winners": 0,
        "losers": 0,
        "breakevenCount": 0,
        "winRatePercent": 0.0,
        "lossRatePercent": 0.0,
        "averageWinPercent": 0.0,
        "averageLossPercent": 0.0,
        "averageWinValue": 0.0,
        "averageLossValue": 0.0,
        "grossProfitValue": 0.0,
        "grossLossValue": 0.0,
        "grossProfitPercent": 0.0,
        "grossLossPercent": 0.0,
        "profitFactor": 0.0,
        "expectancyPercent": 0.0,
        "expectancyValue": 0.0,
        "bestTradePercent": 0.0,
        "worstTradePercent": 0.0,
        "bestTradeValue": 0.0,
        "worstTradeValue": 0.0,
        "slTpTotalPnlPercent": 0.0,
        "slTpAveragePnlPercent": 0.0,
        "slTpTotalPnlValue": 0.0,
        "slTpCount": 0,
        "targetedCount": 0,
        "slTpWinners": 0,
        "slTpLosers": 0,
        "slTpBreakevenCount": 0,
        "slTpWinRatePercent": 0.0,
        "slTpLossRatePercent": 0.0,
        "tpHitRatePercent": 0.0,
        "slHitRatePercent": 0.0,
        "slTpAverageWinPercent": 0.0,
        "slTpAverageLossPercent": 0.0,
        "slTpProfitFactor": 0.0,
        "slTpExpectancyPercent": 0.0,
        "invalidStopCount": 0,
        "invalidTargetCount": 0,
    }


class PositionService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def _refresh_open_marks(self, rows: list[Position]) -> None:
        open_rows = [row for row in rows if row.status == "open"]
        if not open_rows:
            return
        symbols = [row.symbol for row in open_rows]
        asset_rows = list(self.db.scalars(select(AssetStateCurrent).where(AssetStateCurrent.symbol.in_(symbols))).all())
        asset_by_symbol = {row.symbol.upper(): row for row in asset_rows}
        changed = False
        for row in open_rows:
            asset = asset_by_symbol.get(row.symbol.upper())
            if not asset or asset.price is None:
                continue
            row.mark_price = float(asset.price)
            row.unrealized_pnl = _position_pnl(side=row.side, entry_price=row.entry_price, mark_price=row.mark_price, quantity=row.quantity)
            changed = True
        if changed:
            self.db.commit()
            for row in open_rows:
                self.db.refresh(row)

    def list_positions(self, limit: int = 100, status: str | None = None) -> list[Position]:
        stmt = select(Position)
        if status:
            stmt = stmt.where(Position.status == status)
        stmt = stmt.order_by(Position.opened_at.desc()).limit(limit)
        rows = list(self.db.scalars(stmt).all())
        self._refresh_open_marks(rows)
        return rows

    def clear_positions(self, status: str | None = None) -> int:
        stmt = delete(Position)
        if status:
            stmt = stmt.where(Position.status == status)
        result = self.db.execute(stmt)
        self.db.commit()
        return int(result.rowcount or 0)

    def pnl_summary(self, status: str | None = None) -> dict:
        stmt = select(Position)
        if status:
            stmt = stmt.where(Position.status == status)
        rows = list(self.db.scalars(stmt).all())
        self._refresh_open_marks(rows)

        summary = _empty_summary()
        winning_pct_total = losing_pct_total = 0.0
        winning_value_total = losing_value_total = 0.0
        sl_tp_winning_pct_total = sl_tp_losing_pct_total = 0.0
        sl_tp_winning_value_total = sl_tp_losing_value_total = 0.0
        best_pct = worst_pct = None
        best_value = worst_value = None

        for row in rows:
            if row.stop_price is not None and not _is_valid_stop(row):
                summary["invalidStopCount"] += 1
            if row.target_price is not None and not _is_valid_target(row):
                summary["invalidTargetCount"] += 1

            effective_price = _effective_pnl_price(row)
            pct = _pnl_pct_from_price(row, effective_price)
            pnl = _pnl_from_price(row, effective_price)
            if pct is None or pnl is None:
                continue

            summary["totalPnlPercent"] += pct
            summary["totalPnlValue"] += pnl
            summary["count"] += 1
            best_pct = pct if best_pct is None else max(best_pct, pct)
            worst_pct = pct if worst_pct is None else min(worst_pct, pct)
            best_value = pnl if best_value is None else max(best_value, pnl)
            worst_value = pnl if worst_value is None else min(worst_value, pnl)

            if pct > 0:
                summary["winners"] += 1
                winning_pct_total += pct
                winning_value_total += pnl
            elif pct < 0:
                summary["losers"] += 1
                losing_pct_total += pct
                losing_value_total += pnl
            else:
                summary["breakevenCount"] += 1
            if _has_triggered_stop(row):
                summary["stoppedCount"] += 1

            sl_tp_price = _effective_sl_tp_pnl_price(row)
            sl_tp_pct = _pnl_pct_from_price(row, sl_tp_price)
            sl_tp_pnl = _pnl_from_price(row, sl_tp_price)
            if sl_tp_pct is None or sl_tp_pnl is None:
                continue
            summary["slTpTotalPnlPercent"] += sl_tp_pct
            summary["slTpTotalPnlValue"] += sl_tp_pnl
            summary["slTpCount"] += 1
            if sl_tp_pct > 0:
                summary["slTpWinners"] += 1
                sl_tp_winning_pct_total += sl_tp_pct
                sl_tp_winning_value_total += sl_tp_pnl
            elif sl_tp_pct < 0:
                summary["slTpLosers"] += 1
                sl_tp_losing_pct_total += sl_tp_pct
                sl_tp_losing_value_total += sl_tp_pnl
            else:
                summary["slTpBreakevenCount"] += 1
            if not _has_triggered_stop(row) and _has_triggered_target(row):
                summary["targetedCount"] += 1

        count = summary["count"]
        if count > 0:
            summary["averagePnlPercent"] = summary["totalPnlPercent"] / count
            summary["expectancyPercent"] = summary["averagePnlPercent"]
            summary["expectancyValue"] = summary["totalPnlValue"] / count
            summary["winRatePercent"] = _safe_div(summary["winners"], count) * 100
            summary["lossRatePercent"] = _safe_div(summary["losers"], count) * 100
            summary["bestTradePercent"] = best_pct or 0.0
            summary["worstTradePercent"] = worst_pct or 0.0
            summary["bestTradeValue"] = best_value or 0.0
            summary["worstTradeValue"] = worst_value or 0.0

        if summary["winners"] > 0:
            summary["averageWinPercent"] = winning_pct_total / summary["winners"]
            summary["averageWinValue"] = winning_value_total / summary["winners"]
        if summary["losers"] > 0:
            summary["averageLossPercent"] = losing_pct_total / summary["losers"]
            summary["averageLossValue"] = losing_value_total / summary["losers"]
        summary["grossProfitValue"] = winning_value_total
        summary["grossLossValue"] = losing_value_total
        summary["grossProfitPercent"] = winning_pct_total
        summary["grossLossPercent"] = losing_pct_total
        summary["profitFactor"] = _safe_div(winning_value_total, abs(losing_value_total)) if losing_value_total else (winning_value_total if winning_value_total else 0.0)

        sl_tp_count = summary["slTpCount"]
        if sl_tp_count > 0:
            summary["slTpAveragePnlPercent"] = summary["slTpTotalPnlPercent"] / sl_tp_count
            summary["slTpExpectancyPercent"] = summary["slTpAveragePnlPercent"]
            summary["slTpWinRatePercent"] = _safe_div(summary["slTpWinners"], sl_tp_count) * 100
            summary["slTpLossRatePercent"] = _safe_div(summary["slTpLosers"], sl_tp_count) * 100
            summary["tpHitRatePercent"] = _safe_div(summary["targetedCount"], sl_tp_count) * 100
            summary["slHitRatePercent"] = _safe_div(summary["stoppedCount"], sl_tp_count) * 100
        if summary["slTpWinners"] > 0:
            summary["slTpAverageWinPercent"] = sl_tp_winning_pct_total / summary["slTpWinners"]
        if summary["slTpLosers"] > 0:
            summary["slTpAverageLossPercent"] = sl_tp_losing_pct_total / summary["slTpLosers"]
        summary["slTpProfitFactor"] = _safe_div(sl_tp_winning_value_total, abs(sl_tp_losing_value_total)) if sl_tp_losing_value_total else (sl_tp_winning_value_total if sl_tp_winning_value_total else 0.0)
        return summary

    def get_open_position_for_candidate(self, candidate_id: str | None) -> Position | None:
        if not candidate_id:
            return None
        rows = list(self.db.scalars(select(Position).where(Position.status == "open")).all())
        for row in rows:
            meta = row.meta or {}
            if meta.get("candidate_id") == candidate_id:
                return row
        return None

    def get_open_position_for_symbol(self, symbol: str, side: str | None = None) -> Position | None:
        stmt = select(Position).where(Position.status == "open", Position.symbol == symbol.upper())
        if side:
            stmt = stmt.where(Position.side == side)
        stmt = stmt.order_by(Position.opened_at.desc()).limit(1)
        return self.db.scalars(stmt).first()

    def create_position(self, *, symbol: str, side: str, quantity: float, entry_price: float | None, mark_price: float | None, stop_price: float | None, target_price: float | None, meta: dict | None) -> Position:
        meta = meta or {}
        candidate_id = meta.get("candidate_id")
        existing = self.get_open_position_for_candidate(candidate_id)
        if existing is None:
            existing = self.get_open_position_for_symbol(symbol, side=side)
        if existing is not None:
            existing.mark_price = mark_price if mark_price is not None else existing.mark_price
            existing.stop_price = stop_price if stop_price is not None else existing.stop_price
            existing.target_price = target_price if target_price is not None else existing.target_price
            existing.unrealized_pnl = _position_pnl(side=existing.side, entry_price=existing.entry_price, mark_price=existing.mark_price, quantity=existing.quantity)
            existing.meta = {**(existing.meta or {}), **meta, "dedupe_refresh": True}
            self.db.commit()
            self.db.refresh(existing)
            return existing

        row = Position(
            position_id=f"pos_{uuid4().hex[:16]}",
            symbol=symbol.upper(),
            side=side,
            quantity=quantity,
            entry_price=entry_price,
            mark_price=mark_price,
            stop_price=stop_price,
            target_price=target_price,
            unrealized_pnl=_position_pnl(side=side, entry_price=entry_price, mark_price=mark_price, quantity=quantity),
            status="open",
            meta=meta,
        )
        self.db.add(row)
        self.db.commit()
        self.db.refresh(row)
        return row

    def close_position(self, position_id: str, *, mark_price: float | None, unrealized_pnl: float | None = None) -> Position | None:
        row = self.db.get(Position, position_id)
        if row is None:
            return None
        row.status = "closed"
        row.mark_price = mark_price
        row.unrealized_pnl = unrealized_pnl if unrealized_pnl is not None else _position_pnl(side=row.side, entry_price=row.entry_price, mark_price=mark_price, quantity=row.quantity)
        row.closed_at = datetime.now(timezone.utc)
        self.db.commit()
        self.db.refresh(row)
        return row

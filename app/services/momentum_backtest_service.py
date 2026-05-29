from __future__ import annotations

from datetime import datetime, timezone
from statistics import fmean
from typing import Any
from uuid import uuid4

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.models.market_candle import MarketCandle
from app.models.momentum_backtest import MomentumBacktestEquity, MomentumBacktestRun, MomentumBacktestTrade


class MomentumBacktestService:
    DEFAULT_SETTINGS = {
        "initial_capital": 1000.0,
        "entry_rsi_min": 45.0,
        "entry_rsi_max": 55.0,
        "entry_pool_top_n": 10,
        "entry_pool_min_leader_ratio": 0.80,
        "fee_pct": 0.001,
        "slippage_pct": 0.0005,
        "max_symbols": 300,
        "warmup_15m": 96,
        "decision_stride": 4,
    }

    def __init__(self, db: Session) -> None:
        self.db = db

    def create_run(self, settings: dict[str, Any] | None = None) -> MomentumBacktestRun:
        active = self.db.scalars(
            select(MomentumBacktestRun)
            .where(MomentumBacktestRun.status.in_(["queued", "running"]))
            .order_by(MomentumBacktestRun.created_at.desc())
            .limit(1)
        ).first()
        if active:
            return active
        payload = {**self.DEFAULT_SETTINGS, **(settings or {})}
        run = MomentumBacktestRun(
            run_id=f"mombacktest-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:6]}",
            status="queued",
            initial_capital=float(payload["initial_capital"]),
            settings=payload,
        )
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        return run

    def latest_run(self) -> MomentumBacktestRun | None:
        return self.db.scalars(select(MomentumBacktestRun).order_by(MomentumBacktestRun.created_at.desc()).limit(1)).first()

    def list_trades(self, run_id: str, *, limit: int = 300) -> list[MomentumBacktestTrade]:
        return list(self.db.scalars(
            select(MomentumBacktestTrade).where(MomentumBacktestTrade.run_id == run_id).order_by(MomentumBacktestTrade.entry_time.desc()).limit(limit)
        ).all())

    def list_equity(self, run_id: str, *, limit: int = 1000) -> list[MomentumBacktestEquity]:
        rows = list(self.db.scalars(
            select(MomentumBacktestEquity).where(MomentumBacktestEquity.run_id == run_id).order_by(MomentumBacktestEquity.timestamp.asc())
        ).all())
        if len(rows) <= limit:
            return rows
        step = max(1, len(rows) // limit)
        return rows[::step][:limit]

    def process_next_queued(self) -> dict[str, Any] | None:
        run = self.db.scalars(
            select(MomentumBacktestRun).where(MomentumBacktestRun.status == "queued").order_by(MomentumBacktestRun.created_at.asc()).limit(1)
        ).first()
        if not run:
            return None
        return self.run_backtest(run.run_id)

    def run_backtest(self, run_id: str) -> dict[str, Any]:
        run = self.db.get(MomentumBacktestRun, run_id)
        if not run:
            raise ValueError(f"Unknown backtest run {run_id}")
        settings = {**self.DEFAULT_SETTINGS, **(run.settings or {})}
        run.status = "running"
        run.started_at = datetime.now(timezone.utc)
        run.updated_at = datetime.now(timezone.utc)
        run.error = None
        self.db.execute(delete(MomentumBacktestTrade).where(MomentumBacktestTrade.run_id == run_id))
        self.db.execute(delete(MomentumBacktestEquity).where(MomentumBacktestEquity.run_id == run_id))
        self.db.commit()

        try:
            symbols = self._symbols(limit=int(settings["max_symbols"]))
            run.symbols_total = len(symbols)
            self.db.commit()
            data = {symbol: self._load_symbol(symbol) for symbol in symbols}
            data = {symbol: bundle for symbol, bundle in data.items() if len(bundle["15m"]) >= int(settings["warmup_15m"]) and len(bundle["1h"]) >= 20 and len(bundle["4h"]) >= 10}
            symbols = list(data.keys())
            if not symbols:
                raise ValueError("No symbols have enough 15m/1h/4h candles for backtest")

            timeline = sorted(set.intersection(*[set(c["close_time"] for c in bundle["15m"]) for bundle in data.values()]))
            if len(timeline) < int(settings["warmup_15m"]):
                # Fall back to global timeline when symbols do not perfectly overlap.
                timeline = sorted({c["close_time"] for bundle in data.values() for c in bundle["15m"]})

            cash = float(settings["initial_capital"])
            position: dict[str, Any] | None = None
            peak_equity = cash
            max_dd = 0.0
            wins = 0
            losses = 0
            gross_profit = 0.0
            gross_loss = 0.0
            trade_count = 0
            stride = max(1, int(settings["decision_stride"]))

            for index, ts in enumerate(timeline):
                if index < int(settings["warmup_15m"]) or index % stride != 0:
                    continue
                snapshot = self._snapshot(data, ts, settings)
                if not snapshot:
                    continue
                current = snapshot.get(position["symbol"]) if position else None
                next_entry = self._best_entry(snapshot, settings, exclude={position["symbol"]} if position else set())

                if position:
                    current_price = float((current or {}).get("price") or position["entry_price"])
                    should_exit = self._structure_broken(current) or bool(next_entry)
                    if should_exit:
                        cash, pnl, pnl_pct = self._close_trade(run_id, position, ts, current_price, cash, current, settings, reason="structure_break" if self._structure_broken(current) else "next_entry_ready")
                        trade_count += 1
                        if pnl >= 0:
                            wins += 1
                            gross_profit += pnl
                        else:
                            losses += 1
                            gross_loss += abs(pnl)
                        position = None

                if position is None and next_entry:
                    position = self._open_position(next_entry, ts, cash, settings)
                    cash = 0.0

                mark = float((snapshot.get(position["symbol"]) or {}).get("price") or position["entry_price"]) if position else 0.0
                equity = cash + (position["quantity"] * mark if position else 0.0)
                peak_equity = max(peak_equity, equity)
                dd = ((peak_equity - equity) / peak_equity * 100.0) if peak_equity else 0.0
                max_dd = max(max_dd, dd)
                self.db.add(MomentumBacktestEquity(
                    run_id=run_id,
                    timestamp=datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc),
                    equity=equity,
                    cash=cash,
                    position_symbol=position["symbol"] if position else None,
                    position_value=position["quantity"] * mark if position else 0.0,
                    drawdown_pct=dd,
                ))

                if index % 200 == 0:
                    run.symbols_processed = len(symbols)
                    run.updated_at = datetime.now(timezone.utc)
                    self.db.commit()

            if position:
                last_snapshot = self._snapshot(data, timeline[-1], settings)
                last_price = float((last_snapshot.get(position["symbol"]) or {}).get("price") or position["entry_price"])
                cash, pnl, pnl_pct = self._close_trade(run_id, position, timeline[-1], last_price, cash, last_snapshot.get(position["symbol"]), settings, reason="end_of_backtest")
                trade_count += 1
                if pnl >= 0:
                    wins += 1
                    gross_profit += pnl
                else:
                    losses += 1
                    gross_loss += abs(pnl)

            final_equity = cash
            initial = float(settings["initial_capital"])
            total_pnl = final_equity - initial
            run.status = "completed"
            run.symbols_total = len(symbols)
            run.symbols_processed = len(symbols)
            run.final_equity = final_equity
            run.total_pnl = total_pnl
            run.total_pnl_pct = (total_pnl / initial * 100.0) if initial else 0.0
            run.max_drawdown_pct = max_dd
            run.trade_count = trade_count
            run.winrate = (wins / trade_count * 100.0) if trade_count else 0.0
            run.profit_factor = (gross_profit / gross_loss) if gross_loss else (gross_profit if gross_profit else 0.0)
            run.completed_at = datetime.now(timezone.utc)
            run.updated_at = datetime.now(timezone.utc)
            self.db.commit()
            return {"run_id": run_id, "status": run.status, "trades": trade_count, "final_equity": final_equity}
        except Exception as exc:
            run.status = "failed"
            run.error = str(exc)
            run.completed_at = datetime.now(timezone.utc)
            run.updated_at = datetime.now(timezone.utc)
            self.db.commit()
            raise

    def _symbols(self, *, limit: int) -> list[str]:
        stmt = select(MarketCandle.symbol).distinct().order_by(MarketCandle.symbol).limit(limit)
        return [s.upper() for s in self.db.scalars(stmt).all()]

    def _load_symbol(self, symbol: str) -> dict[str, list[dict[str, Any]]]:
        return {interval: self._candles(symbol, interval) for interval in ("15m", "1h", "4h")}

    def _candles(self, symbol: str, interval: str) -> list[dict[str, Any]]:
        rows = list(self.db.scalars(select(MarketCandle).where(MarketCandle.symbol == symbol, MarketCandle.interval == interval).order_by(MarketCandle.close_time.asc())).all())
        return [{"open_time": r.open_time, "close_time": r.close_time, "open": r.open, "high": r.high, "low": r.low, "close": r.close} for r in rows]

    def _snapshot(self, data: dict[str, dict[str, list[dict[str, Any]]]], ts: int, settings: dict[str, Any]) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for symbol, bundle in data.items():
            c15 = [c for c in bundle["15m"] if c["close_time"] <= ts]
            c1h = [c for c in bundle["1h"] if c["close_time"] <= ts]
            c4h = [c for c in bundle["4h"] if c["close_time"] <= ts]
            if len(c15) < 32 or len(c1h) < 16 or len(c4h) < 10:
                continue
            row = self._rank_row(symbol, c15[-32:], c1h[-24:], c4h[-30:])
            out[symbol] = row
        ranked = sorted(out.values(), key=lambda r: r["momentum_score"], reverse=True)
        for i, row in enumerate(ranked, start=1):
            row["rank"] = i
        return {row["symbol"]: row for row in ranked}

    def _rank_row(self, symbol: str, c15: list[dict], c1h: list[dict], c4h: list[dict]) -> dict[str, Any]:
        m15 = self._momentum(c15)
        m1h = self._momentum(c1h)
        m4h = self._momentum(c4h)
        values = [(m15, 0.35), (m1h, 0.40), (m4h, 0.25)]
        weights = sum(w for value, w in values if value is not None)
        score = sum((value or 0.0) * w for value, w in values) / weights if weights else 0.0
        structure = self._structure(c15)
        return {
            "symbol": symbol,
            "price": float(c15[-1]["close"]),
            "momentum_score": score,
            "momentum_15m": m15,
            "momentum_1h": m1h,
            "momentum_4h": m4h,
            "rsi_1h": self._rsi([float(c["close"]) for c in c1h]),
            **structure,
        }

    def _momentum(self, candles: list[dict]) -> float | None:
        if len(candles) < 2 or float(candles[0]["close"]) == 0:
            return None
        closes = [float(c["close"]) for c in candles]
        change = ((closes[-1] - closes[0]) / closes[0]) * 100.0
        rsi = self._rsi(closes)
        return change + (((rsi or 50.0) - 50.0) / 2.0)

    def _rsi(self, closes: list[float], period: int = 14) -> float | None:
        if len(closes) <= period:
            return None
        changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        recent = changes[-period:]
        gains = [max(c, 0.0) for c in recent]
        losses = [abs(min(c, 0.0)) for c in recent]
        avg_gain = fmean(gains)
        avg_loss = fmean(losses)
        if avg_loss == 0:
            return 100.0 if avg_gain > 0 else 50.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def _structure(self, candles: list[dict]) -> dict[str, Any]:
        lows = [float(c["low"]) for c in candles]
        highs = [float(c["high"]) for c in candles]
        closes = [float(c["close"]) for c in candles]
        last_swing_low = min(lows[-10:-1]) if len(lows) >= 11 else min(lows[:-1])
        last_swing_high = max(highs[-10:-1]) if len(highs) >= 11 else max(highs[:-1])
        broken = closes[-1] < last_swing_low
        return {
            "structure_15m_status": "broken_bearish" if broken else "valid",
            "bos_15m_bearish": broken,
            "last_swing_low_15m": last_swing_low,
            "last_swing_high_15m": last_swing_high,
        }

    def _in_pool(self, row: dict[str, Any], leader_score: float, settings: dict[str, Any]) -> bool:
        return int(row.get("rank") or 9999) <= int(settings["entry_pool_top_n"]) or (leader_score > 0 and float(row.get("momentum_score") or 0) >= leader_score * float(settings["entry_pool_min_leader_ratio"]))

    def _entry_ready(self, row: dict[str, Any], settings: dict[str, Any]) -> bool:
        rsi = row.get("rsi_1h")
        return row.get("structure_15m_status") == "valid" and rsi is not None and float(settings["entry_rsi_min"]) <= float(rsi) <= float(settings["entry_rsi_max"])

    def _best_entry(self, snapshot: dict[str, dict[str, Any]], settings: dict[str, Any], *, exclude: set[str]) -> dict[str, Any] | None:
        ranked = sorted(snapshot.values(), key=lambda r: r["momentum_score"], reverse=True)
        leader = float(ranked[0]["momentum_score"]) if ranked else 0.0
        for row in ranked:
            if row["symbol"] in exclude:
                continue
            if self._in_pool(row, leader, settings) and self._entry_ready(row, settings):
                return row
        return None

    def _structure_broken(self, row: dict[str, Any] | None) -> bool:
        return row is None or row.get("structure_15m_status") == "broken_bearish" or bool(row.get("bos_15m_bearish"))

    def _open_position(self, row: dict[str, Any], ts: int, cash: float, settings: dict[str, Any]) -> dict[str, Any]:
        fee = float(settings["fee_pct"])
        slippage = float(settings["slippage_pct"])
        entry_price = float(row["price"]) * (1.0 + slippage)
        entry_value = cash * (1.0 - fee)
        return {"symbol": row["symbol"], "entry_time": ts, "entry_price": entry_price, "quantity": entry_value / entry_price, "entry_value": entry_value, "rank": row.get("rank"), "momentum_score": row.get("momentum_score"), "entry_rsi_1h": row.get("rsi_1h")}

    def _close_trade(self, run_id: str, position: dict[str, Any], ts: int, price: float, cash: float, row: dict[str, Any] | None, settings: dict[str, Any], *, reason: str) -> tuple[float, float, float]:
        fee = float(settings["fee_pct"])
        slippage = float(settings["slippage_pct"])
        exit_price = float(price) * (1.0 - slippage)
        gross_exit = float(position["quantity"]) * exit_price
        exit_value = gross_exit * (1.0 - fee)
        pnl = exit_value - float(position["entry_value"])
        pnl_pct = (pnl / float(position["entry_value"]) * 100.0) if position["entry_value"] else 0.0
        self.db.add(MomentumBacktestTrade(
            trade_id=f"bttrade-{uuid4().hex}",
            run_id=run_id,
            symbol=position["symbol"],
            entry_time=datetime.fromtimestamp(position["entry_time"] / 1000.0, tz=timezone.utc),
            entry_price=float(position["entry_price"]),
            exit_time=datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc),
            exit_price=exit_price,
            quantity=float(position["quantity"]),
            entry_value=float(position["entry_value"]),
            exit_value=exit_value,
            pnl=pnl,
            pnl_pct=pnl_pct,
            reason=reason,
            entry_rsi_1h=position.get("entry_rsi_1h"),
            exit_structure_15m=(row or {}).get("structure_15m_status"),
            momentum_score=position.get("momentum_score"),
            rank=position.get("rank"),
        ))
        return exit_value, pnl, pnl_pct

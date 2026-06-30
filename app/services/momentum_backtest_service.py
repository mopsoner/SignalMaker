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
        "name": "P3 RSI 1h 50-62 + 4h momentum + 5m exit",
        "initial_capital": 1000.0,
        "entry_rsi_min": 50.0,
        "entry_rsi_max": 62.0,
        "entry_pool_top_n": 10,
        "entry_pool_min_leader_ratio": 0.80,
        "require_momentum_4h_positive": True,
        "require_entry_structure_15m_valid": True,
        "exit_structure_interval": "5m",
        "fee_pct": 0.001,
        "slippage_pct": 0.0005,
        "max_symbols": 300,
        "warmup_candles": 96,
        "decision_stride": 3,
        "volume_score_enabled": True,
        "volume_score_weight": 0.15,
        "volume_score_cap": 12.0,
    }

    def __init__(self, db: Session) -> None:
        self.db = db

    def create_run(self, settings: dict[str, Any] | None = None) -> MomentumBacktestRun:
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

    def create_rsi_sweep(self, ranges: list[dict[str, float]], base_settings: dict[str, Any] | None = None) -> list[MomentumBacktestRun]:
        runs: list[MomentumBacktestRun] = []
        base = {**self.DEFAULT_SETTINGS, **(base_settings or {})}
        for item in ranges:
            rmin = float(item["min"])
            rmax = float(item["max"])
            settings = {**base, "entry_rsi_min": rmin, "entry_rsi_max": rmax, "name": f"RSI 1h {rmin:g}-{rmax:g}"}
            runs.append(self.create_run(settings))
        return runs

    def list_runs(self, *, limit: int = 20) -> list[MomentumBacktestRun]:
        return list(self.db.scalars(select(MomentumBacktestRun).order_by(MomentumBacktestRun.created_at.desc()).limit(limit)).all())

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
        exit_tf = str(settings.get("exit_structure_interval") or "5m")
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
            data = {symbol: self._load_symbol(symbol, exit_tf=exit_tf) for symbol in symbols}
            data = {
                symbol: bundle
                for symbol, bundle in data.items()
                if len(bundle.get(exit_tf, [])) >= int(settings["warmup_candles"])
                and len(bundle.get("15m", [])) >= 32
                and len(bundle.get("1h", [])) >= 20
                and len(bundle.get("4h", [])) >= 10
            }
            symbols = list(data.keys())
            if not symbols:
                raise ValueError(f"No symbols have enough {exit_tf}/15m/1h/4h candles for backtest")

            timeline = sorted({c["close_time"] for bundle in data.values() for c in bundle[exit_tf]})
            cash = float(settings["initial_capital"])
            position: dict[str, Any] | None = None
            peak_equity = cash
            max_dd = 0.0
            wins = 0
            gross_profit = 0.0
            gross_loss = 0.0
            trade_count = 0
            stride = max(1, int(settings["decision_stride"]))

            for index, ts in enumerate(timeline):
                if index < int(settings["warmup_candles"]) or index % stride != 0:
                    continue
                snapshot = self._snapshot(data, ts, settings, exit_tf=exit_tf)
                if not snapshot:
                    continue
                current = snapshot.get(position["symbol"]) if position else None
                next_entry = self._best_entry(snapshot, settings, exclude={position["symbol"]} if position else set())

                if position:
                    current_price = float((current or {}).get("price") or position["entry_price"])
                    structure_break = self._structure_broken(current)
                    should_exit = structure_break or bool(next_entry)
                    if should_exit:
                        cash, pnl, _ = self._close_trade(
                            run_id,
                            position,
                            ts,
                            current_price,
                            current,
                            settings,
                            reason="structure_5m_break" if structure_break and exit_tf == "5m" else "structure_break" if structure_break else "next_entry_ready",
                        )
                        trade_count += 1
                        if pnl >= 0:
                            wins += 1
                            gross_profit += pnl
                        else:
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
                if index % 300 == 0:
                    run.symbols_processed = len(symbols)
                    run.updated_at = datetime.now(timezone.utc)
                    self.db.commit()

            if position:
                last_snapshot = self._snapshot(data, timeline[-1], settings, exit_tf=exit_tf)
                last_price = float((last_snapshot.get(position["symbol"]) or {}).get("price") or position["entry_price"])
                cash, pnl, _ = self._close_trade(run_id, position, timeline[-1], last_price, last_snapshot.get(position["symbol"]), settings, reason="end_of_backtest")
                trade_count += 1
                if pnl >= 0:
                    wins += 1
                    gross_profit += pnl
                else:
                    gross_loss += abs(pnl)

            initial = float(settings["initial_capital"])
            total_pnl = cash - initial
            run.status = "completed"
            run.symbols_total = len(symbols)
            run.symbols_processed = len(symbols)
            run.final_equity = cash
            run.total_pnl = total_pnl
            run.total_pnl_pct = (total_pnl / initial * 100.0) if initial else 0.0
            run.max_drawdown_pct = max_dd
            run.trade_count = trade_count
            run.winrate = (wins / trade_count * 100.0) if trade_count else 0.0
            run.profit_factor = (gross_profit / gross_loss) if gross_loss else (gross_profit if gross_profit else 0.0)
            run.completed_at = datetime.now(timezone.utc)
            run.updated_at = datetime.now(timezone.utc)
            self.db.commit()
            return {"run_id": run_id, "status": run.status, "trades": trade_count, "final_equity": cash}
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

    def _load_symbol(self, symbol: str, *, exit_tf: str) -> dict[str, list[dict[str, Any]]]:
        intervals = {exit_tf, "15m", "1h", "4h"}
        return {interval: self._candles(symbol, interval) for interval in intervals}

    def _candles(self, symbol: str, interval: str) -> list[dict[str, Any]]:
        rows = list(self.db.scalars(select(MarketCandle).where(MarketCandle.symbol == symbol, MarketCandle.interval == interval).order_by(MarketCandle.close_time.asc())).all())
        return [{"open_time": r.open_time, "close_time": r.close_time, "open": r.open, "high": r.high, "low": r.low, "close": r.close, "volume": r.volume, "quote_volume": getattr(r, "quote_volume", 0.0)} for r in rows]

    def _snapshot(self, data: dict[str, dict[str, list[dict[str, Any]]]], ts: int, settings: dict[str, Any], *, exit_tf: str) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for symbol, bundle in data.items():
            c_exit = [c for c in bundle[exit_tf] if c["close_time"] <= ts]
            c15 = [c for c in bundle["15m"] if c["close_time"] <= ts]
            c1h = [c for c in bundle["1h"] if c["close_time"] <= ts]
            c4h = [c for c in bundle["4h"] if c["close_time"] <= ts]
            if len(c_exit) < 20 or len(c15) < 32 or len(c1h) < 16 or len(c4h) < 10:
                continue
            row = self._rank_row(symbol, c_exit[-32:], c15[-32:], c1h[-24:], c4h[-30:], settings, exit_tf=exit_tf)
            out[symbol] = row
        ranked = sorted(out.values(), key=lambda r: r["momentum_score"], reverse=True)
        for i, row in enumerate(ranked, start=1):
            row["rank"] = i
        return {row["symbol"]: row for row in ranked}

    def _rank_row(self, symbol: str, c_exit: list[dict], c15: list[dict], c1h: list[dict], c4h: list[dict], settings: dict[str, Any], *, exit_tf: str) -> dict[str, Any]:
        m15 = self._momentum(c15)
        m1h = self._momentum(c1h)
        m4h = self._momentum(c4h)
        volume_score = self._volume_score(c1h, settings) if settings.get("volume_score_enabled", True) else 0.0
        values = [(m15, 0.30), (m1h, 0.38), (m4h, 0.25), (volume_score, float(settings.get("volume_score_weight", 0.15)))]
        weights = sum(w for value, w in values if value is not None)
        score = sum((value or 0.0) * w for value, w in values) / weights if weights else 0.0
        exit_structure = self._structure(c_exit, prefix=f"structure_{exit_tf}")
        entry_structure = self._structure(c15, prefix="structure_15m")
        return {
            "symbol": symbol,
            "price": float(c_exit[-1]["close"]),
            "momentum_score": score,
            "momentum_15m": m15,
            "momentum_1h": m1h,
            "momentum_4h": m4h,
            "volume_score": volume_score,
            "rsi_1h": self._rsi([float(c["close"]) for c in c1h]),
            **exit_structure,
            **entry_structure,
        }

    def _momentum(self, candles: list[dict]) -> float | None:
        if len(candles) < 2 or float(candles[0]["close"]) == 0:
            return None
        closes = [float(c["close"]) for c in candles]
        change = ((closes[-1] - closes[0]) / closes[0]) * 100.0
        rsi = self._rsi(closes)
        return change + (((rsi or 50.0) - 50.0) / 2.0)

    def _volume_score(self, candles: list[dict], settings: dict[str, Any]) -> float:
        if len(candles) < 8:
            return 0.0
        volumes = [float(c.get("quote_volume") or c.get("volume") or 0.0) for c in candles]
        avg = fmean(volumes[:-1]) if len(volumes) > 1 else 0.0
        if avg <= 0:
            return 0.0
        raw = ((volumes[-1] / avg) - 1.0) * 10.0
        cap = float(settings.get("volume_score_cap", 12.0))
        return max(-cap, min(cap, raw))

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
        return 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))

    def _structure(self, candles: list[dict], *, prefix: str) -> dict[str, Any]:
        lows = [float(c["low"]) for c in candles]
        highs = [float(c["high"]) for c in candles]
        closes = [float(c["close"]) for c in candles]
        last_swing_low = min(lows[-10:-1]) if len(lows) >= 11 else min(lows[:-1])
        last_swing_high = max(highs[-10:-1]) if len(highs) >= 11 else max(highs[:-1])
        broken = closes[-1] < last_swing_low
        return {
            f"{prefix}_status": "broken_bearish" if broken else "valid",
            f"{prefix}_bearish": broken,
            f"{prefix}_last_swing_low": last_swing_low,
            f"{prefix}_last_swing_high": last_swing_high,
        }

    def _in_pool(self, row: dict[str, Any], leader_score: float, settings: dict[str, Any]) -> bool:
        return int(row.get("rank") or 9999) <= int(settings["entry_pool_top_n"]) or (leader_score > 0 and float(row.get("momentum_score") or 0) >= leader_score * float(settings["entry_pool_min_leader_ratio"]))

    def _entry_ready(self, row: dict[str, Any], settings: dict[str, Any]) -> bool:
        rsi = row.get("rsi_1h")
        rsi_ok = rsi is not None and float(settings["entry_rsi_min"]) <= float(rsi) <= float(settings["entry_rsi_max"])
        structure_ok = (not settings.get("require_entry_structure_15m_valid", True)) or row.get("structure_15m_status") == "valid"
        momentum_4h_ok = (not settings.get("require_momentum_4h_positive", False)) or float(row.get("momentum_4h") or 0.0) > 0
        return structure_ok and momentum_4h_ok and rsi_ok

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
        if not row:
            return True
        exit_tf = str((row.get("exit_structure_interval") or "5m"))
        return row.get("structure_5m_status") == "broken_bearish" or row.get("structure_15m_status") == "broken_bearish" and exit_tf == "15m" or bool(row.get("structure_5m_bearish"))

    def _open_position(self, row: dict[str, Any], ts: int, cash: float, settings: dict[str, Any]) -> dict[str, Any]:
        fee = float(settings["fee_pct"])
        slippage = float(settings["slippage_pct"])
        entry_price = float(row["price"]) * (1.0 + slippage)
        entry_value = cash * (1.0 - fee)
        return {"symbol": row["symbol"], "entry_time": ts, "entry_price": entry_price, "quantity": entry_value / entry_price, "entry_value": entry_value, "rank": row.get("rank"), "momentum_score": row.get("momentum_score"), "entry_rsi_1h": row.get("rsi_1h")}

    def _close_trade(self, run_id: str, position: dict[str, Any], ts: int, price: float, row: dict[str, Any] | None, settings: dict[str, Any], *, reason: str) -> tuple[float, float, float]:
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
            exit_structure_15m=(row or {}).get("structure_15m_status") or (row or {}).get("structure_5m_status"),
            momentum_score=position.get("momentum_score"),
            rank=position.get("rank"),
        ))
        return exit_value, pnl, pnl_pct

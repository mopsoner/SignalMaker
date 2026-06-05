from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.models.market_candle import MarketCandle
from app.models.momentum_backtest import MomentumBacktestEquity, MomentumBacktestRun, MomentumBacktestTrade
from app.services.momentum_engine_service import MomentumEngineService
from app.services.momentum_service import MomentumService


class MomentumBacktestService:
    DEFAULT_SETTINGS = {
        "name": "Live momentum engine rules",
        "initial_capital": 1000.0,
        "fee_pct": 0.001,
        "slippage_pct": 0.0005,
        "max_symbols": 300,
        "warmup_candles": 96,
        "cadence_hours": 4,
        "min_momentum_score": 0.0,
    }
    LEGACY_STRATEGY_SETTING_KEYS = {
        "entry_rsi_min",
        "entry_rsi_max",
        "entry_pool_top_n",
        "entry_pool_min_leader_ratio",
        "require_momentum_4h_positive",
        "require_entry_structure_15m_valid",
        "exit_structure_interval",
        "decision_stride",
        "volume_score_enabled",
        "volume_score_weight",
        "volume_score_cap",
    }

    def __init__(self, db: Session) -> None:
        self.db = db
        self.momentum = MomentumService(db)
        self.engine = MomentumEngineService(db)

    def create_run(self, settings: dict[str, Any] | None = None) -> MomentumBacktestRun:
        payload = self._simulation_settings(settings)
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
        # Backwards-compatible endpoint: strategy parameters are owned by
        # MomentumEngineService, so RSI values sent by older UI clients no
        # longer change the backtest rules. Create comparable live-engine runs
        # while recording the requested ranges as metadata only.
        runs: list[MomentumBacktestRun] = []
        base = self._simulation_settings(base_settings)
        for item in ranges:
            rmin = float(item["min"])
            rmax = float(item["max"])
            settings = {
                **base,
                "name": f"Live engine rules (legacy RSI {rmin:g}-{rmax:g} ignored)",
                "legacy_requested_rsi_min": rmin,
                "legacy_requested_rsi_max": rmax,
            }
            runs.append(self.create_run(settings))
        return runs

    def _simulation_settings(self, settings: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = {**self.DEFAULT_SETTINGS, **(settings or {})}
        for key in self.LEGACY_STRATEGY_SETTING_KEYS:
            payload.pop(key, None)
        return payload

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
        settings = self._simulation_settings(run.settings)
        decision_interval = "15m"
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
            data = {
                symbol: bundle
                for symbol, bundle in data.items()
                if len(bundle.get(decision_interval, [])) >= int(settings["warmup_candles"])
                and len(bundle.get("15m", [])) >= MomentumService.LOOKBACKS["15m"]
                and len(bundle.get("1h", [])) >= MomentumService.RSI_PERIOD + 2
                and len(bundle.get("4h", [])) >= 10
            }
            symbols = list(data.keys())
            if not symbols:
                raise ValueError("No symbols have enough 15m/1h/4h candles for live-engine backtest")

            timeline = sorted({c["close_time"] for bundle in data.values() for c in bundle[decision_interval]})
            cash = float(settings["initial_capital"])
            position: dict[str, Any] | None = None
            peak_equity = cash
            max_dd = 0.0
            wins = 0
            gross_profit = 0.0
            gross_loss = 0.0
            trade_count = 0
            stride = max(1, int(round(float(settings.get("cadence_hours", 4)) * 4)))

            for index, ts in enumerate(timeline):
                if index < int(settings["warmup_candles"]) or index % stride != 0:
                    continue
                snapshot = self._snapshot(data, ts)
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
                            reason="structure_15m_break" if structure_break else "next_entry_ready",
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
                last_snapshot = self._snapshot(data, timeline[-1])
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

    def _load_symbol(self, symbol: str) -> dict[str, list[dict[str, Any]]]:
        return {interval: self._candles(symbol, interval) for interval in MomentumService.INTERVALS}

    def _candles(self, symbol: str, interval: str) -> list[dict[str, Any]]:
        rows = list(self.db.scalars(select(MarketCandle).where(MarketCandle.symbol == symbol, MarketCandle.interval == interval).order_by(MarketCandle.close_time.asc())).all())
        return [
            {
                "open_time": r.open_time,
                "close_time": r.close_time,
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "volume": r.volume,
                "quote_volume": getattr(r, "quote_volume", 0.0),
                "ingested_at": datetime.fromtimestamp(r.close_time / 1000.0, tz=timezone.utc),
            }
            for r in rows
        ]

    def _snapshot(self, data: dict[str, dict[str, list[dict[str, Any]]]], ts: int) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for symbol, bundle in data.items():
            interval_candles = {
                interval: [c for c in bundle[interval] if c["close_time"] <= ts]
                for interval in MomentumService.INTERVALS
            }
            if any(len(interval_candles[interval]) < 2 for interval in MomentumService.INTERVALS):
                continue
            row = self._rank_row(symbol, interval_candles)
            out[symbol] = row
        ranked = sorted(out.values(), key=lambda r: (-float(r["momentum_score"]), r["symbol"]))
        for i, row in enumerate(ranked, start=1):
            row["rank"] = i
        return {row["symbol"]: row for row in ranked}

    def _rank_row(self, symbol: str, interval_candles: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        payload = self.momentum.build_symbol_payload_from_bundle(symbol, interval_candles)
        structure_15m = payload.pop("structure_15m")
        payload.update(structure_15m)
        return payload

    def _best_entry(self, snapshot: dict[str, dict[str, Any]], settings: dict[str, Any], *, exclude: set[str]) -> dict[str, Any] | None:
        ranked = sorted(snapshot.values(), key=lambda r: (-float(r["momentum_score"]), r["symbol"]))
        return self.engine.best_entry_ready_asset(
            rankings=ranked,
            min_momentum_score=float(settings.get("min_momentum_score", 0.0)),
            exclude_symbols=exclude,
        )

    def _structure_broken(self, row: dict[str, Any] | None) -> bool:
        return self.engine.structure_broken(row)

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

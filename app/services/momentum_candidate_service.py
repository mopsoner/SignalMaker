from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.asset_state import AssetStateCurrent
from app.services.momentum_engine_service import MomentumEngineService
from app.services.momentum_service import MomentumService
from app.services.planner_service import PlannerService
from app.services.runtime_settings import load_runtime_settings


class MomentumCandidateService:
    """Build trade-candidate-compatible rows from momentum-ready assets.

    The momentum engine answers "is this asset ready to enter?". This service
    keeps that entry gate, then uses the existing Wyckoff/SMC asset snapshot
    (asset_state_current.state_payload) plus PlannerService's structural stop
    and target selection to produce a trade candidate shape compatible with the
    regular /trade-candidates flow.
    """

    SOURCE = "momentum_candidates"
    CANDIDATE_STATUS = "momentum_ready"
    SIDE = "long"
    STAGE = "momentum_trade"

    def __init__(self, db: Session) -> None:
        self.db = db
        self.momentum = MomentumService(db)
        self.engine = MomentumEngineService(db)
        self.planner = PlannerService()

    def list_candidates(
        self,
        *,
        limit: int = 100,
        min_momentum_score: float = 0.0,
        min_rr: float | None = None,
        require_wyckoff_context: bool = True,
    ) -> list[dict[str, Any]]:
        rankings = self.momentum.list_rankings(limit=300)
        if min_rr is None:
            min_rr = float(load_runtime_settings()["strategy"]["planner_min_rr"])

        ready_assets = self._ready_assets(rankings=rankings, min_momentum_score=min_momentum_score)
        state_by_symbol = self._state_map([asset["symbol"] for asset in ready_assets])

        candidates: list[dict[str, Any]] = []
        for asset in ready_assets:
            if len(candidates) >= limit:
                break
            state = state_by_symbol.get(asset["symbol"])
            candidate = self._candidate_from_ready_asset(
                asset,
                state=state,
                min_rr=min_rr,
                require_wyckoff_context=require_wyckoff_context,
            )
            if candidate is not None:
                candidates.append(candidate)
        return candidates

    def _ready_assets(self, *, rankings: list[dict[str, Any]], min_momentum_score: float) -> list[dict[str, Any]]:
        leader_score = self.engine._leader_score(rankings)
        ready: list[dict[str, Any]] = []
        for row in rankings:
            if float(row.get("price") or 0) <= 0:
                continue
            if float(row.get("momentum_score") or 0) <= min_momentum_score:
                continue
            if not self.engine._in_entry_pool(row, leader_score=leader_score):
                continue
            if not self.engine._entry_ready(row):
                continue
            decorated = self.engine._decorate_entry(row)
            if decorated:
                ready.append(decorated)
        return ready

    def _state_map(self, symbols: list[str]) -> dict[str, AssetStateCurrent]:
        if not symbols:
            return {}
        rows = self.db.scalars(select(AssetStateCurrent).where(AssetStateCurrent.symbol.in_(symbols))).all()
        return {row.symbol: row for row in rows}

    def _candidate_from_ready_asset(
        self,
        asset: dict[str, Any],
        *,
        state: AssetStateCurrent | None,
        min_rr: float,
        require_wyckoff_context: bool,
    ) -> dict[str, Any] | None:
        symbol = str(asset["symbol"]).upper()
        entry = self.planner._as_float(asset.get("price"))
        if entry is None:
            return None

        signal = self._signal_from_context(asset, state=state, entry=entry)
        if require_wyckoff_context and not signal.get("wyckoff_context_available"):
            return None

        stop, stop_source, stop_candidates = self.planner._infer_stop(signal, side=self.SIDE, entry=entry)
        if stop is None:
            return None
        target, target_source, target_candidates = self.planner._infer_target(signal, side=self.SIDE, entry=entry)
        if target is None:
            return None

        resolved_trade = {
            **self.planner._side_fields(self.SIDE),
            "entry": entry,
            "stop": stop,
            "target": target,
            "entry_source": "momentum_current.price",
            "stop_source": stop_source,
            "target_source": target_source,
            "stop_candidates": stop_candidates[:8],
            "target_candidates": target_candidates[:8],
            "inferred_by": "momentum_candidate_service_v1",
        }
        risk = abs(entry - stop)
        if risk <= 0:
            return None
        rr = abs(target - entry) / risk
        if rr < min_rr:
            resolved_trade, upgraded_rr = self.planner._upgrade_target_for_min_rr(resolved_trade, signal, min_rr=min_rr)
            if upgraded_rr is None or upgraded_rr < min_rr:
                return None
            target = resolved_trade["target"]
            rr = upgraded_rr

        execution_target = signal.get("execution_target") or {
            "type": resolved_trade.get("target_source") or "structural_target",
            "level": target,
            "source": self.SOURCE,
            "validation": "wyckoff_smc_structural_liquidity",
        }
        payload = {
            **signal,
            "momentum_asset": asset,
            "trade": resolved_trade,
            "planner_trade_plan": {
                "model": "momentum_ready_plus_wyckoff_smc_v1",
                **resolved_trade,
                "rr_ratio": rr,
            },
            "pipeline": {**(signal.get("pipeline") or {}), "momentum": True, "trade": True},
            "stage": self.STAGE,
            "source": self.SOURCE,
            "min_rr": min_rr,
        }

        return {
            "candidate_id": f"momentum-{symbol}-open",
            "symbol": symbol,
            "side": self.SIDE,
            "stage": self.STAGE,
            "status": self.CANDIDATE_STATUS,
            "score": float(asset.get("momentum_score") or 0.0),
            "entry_price": entry,
            "stop_price": float(stop),
            "target_price": float(target),
            "rr_ratio": rr,
            "execution_target": execution_target,
            "liquidity_context": signal.get("liquidity_context"),
            "notes": self._notes(asset, stop_source=stop_source, target_source=target_source),
            "payload": payload,
            "created_at": datetime.now(timezone.utc),
        }

    def _signal_from_context(self, asset: dict[str, Any], *, state: AssetStateCurrent | None, entry: float) -> dict[str, Any]:
        state_payload = dict(state.state_payload or {}) if state and isinstance(state.state_payload, dict) else {}
        signal = {
            **state_payload,
            "symbol": asset["symbol"],
            "price": entry,
            "bias": state_payload.get("bias") or (state.bias if state else None) or "bullish_momentum",
            "score": float(asset.get("momentum_score") or 0.0),
            "momentum_score": float(asset.get("momentum_score") or 0.0),
            "momentum_rank": asset.get("rank"),
            "classification": asset.get("classification"),
            "rsi_1h": asset.get("rsi_1h"),
            "rsi_15m": asset.get("rsi_15m"),
            "execution_target": state_payload.get("execution_target") or (state.execution_target if state else None),
            "liquidity_context": state_payload.get("liquidity_context") or (state.liquidity_context if state else None),
            "entry_liquidity_context": state_payload.get("entry_liquidity_context") or state_payload.get("liquidity_context") or (state.liquidity_context if state else None),
            "wyckoff_context_available": bool(
                state and (state.state_payload or state.execution_target or state.liquidity_context)
            ),
        }

        # Feed the planner with the momentum structure as a safe fallback, while
        # keeping Wyckoff/SMC levels from state_payload first when available.
        if signal.get("external_swing_low") is None:
            signal["external_swing_low"] = asset.get("last_swing_low_15m")
        if signal.get("external_swing_high") is None:
            signal["external_swing_high"] = asset.get("last_swing_high_15m")
        return signal

    def _notes(self, asset: dict[str, Any], *, stop_source: str | None, target_source: str | None) -> str:
        return (
            f"Momentum ready: rank={asset.get('rank')} score={asset.get('momentum_score')} "
            f"RSI {asset.get('entry_rsi_timeframe')}={asset.get('rsi_1h')} "
            f"structure={asset.get('structure_15m_status')}; stop={stop_source}; target={target_source}"
        )

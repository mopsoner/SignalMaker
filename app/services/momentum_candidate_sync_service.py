from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any

import requests
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.trade_candidate import TradeCandidate
from app.services.runtime_settings import get_runtime_momentum_config
from app.services.trade_candidate_service import TradeCandidateService

logger = logging.getLogger(__name__)


class MomentumCandidateSyncService:
    """Synchronize momentum rankings into the local executor trade-candidate backlog."""

    def __init__(self, db: Session) -> None:
        self.db = db
        self.trade_candidates = TradeCandidateService(db)

    def sync(
        self,
        *,
        limit: int | None = None,
        min_momentum_score: float | None = None,
        min_rr: float | None = None,
        require_wyckoff_context: bool | None = None,
    ) -> dict[str, Any]:
        momentum_config = get_runtime_momentum_config(self.db)
        params = self._params(
            momentum_config,
            limit=limit,
            min_momentum_score=min_momentum_score,
            min_rr=min_rr,
            require_wyckoff_context=require_wyckoff_context,
        )
        url = self._url(momentum_config)
        summary: dict[str, Any] = {"fetched": 0, "upserted": 0, "skipped": [], "errors": [], "source_url": url}

        try:
            response = requests.get(
                url,
                params=params,
                timeout=self._float(
                    momentum_config.get("momentum_candidates_http_timeout_sec"),
                    default=settings.momentum_candidates_http_timeout_sec,
                ),
                headers={"accept": "application/json"},
            )
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            message = f"momentum_candidates_api_error: {exc}"
            logger.exception("Momentum candidate sync failed from %s", url)
            summary["errors"].append({"reason": "api_error", "detail": message, "url": url})
            return summary

        candidates = self._candidate_rows(data)
        summary["fetched"] = len(candidates)
        logger.info("Fetched %s momentum rows from %s", summary["fetched"], url)

        effective_min_score = self._effective_min_score(momentum_config, min_momentum_score)
        effective_min_rr = min_rr if min_rr is not None else self._float(momentum_config.get("momentum_candidates_min_rr"))

        for raw_candidate in candidates:
            candidate = self._as_trade_candidate(raw_candidate, momentum_config)
            remote_id = str(candidate.get("candidate_id") or raw_candidate.get("candidate_id") or raw_candidate.get("symbol") or "")
            symbol = str(candidate.get("symbol") or "").upper().strip()
            local_id = self._local_candidate_id(symbol)
            validation_error = self._validation_error(candidate, min_score=effective_min_score, min_rr=effective_min_rr)
            if validation_error:
                summary["skipped"].append({"candidate_id": remote_id, "symbol": symbol, "reason": validation_error})
                logger.info("Skipped momentum candidate %s/%s: %s", remote_id, symbol, validation_error)
                continue

            existing = self.db.get(TradeCandidate, local_id)
            if existing is not None and existing.status == "executed":
                summary["skipped"].append({"candidate_id": remote_id, "local_candidate_id": local_id, "reason": "local_candidate_already_executed"})
                logger.info("Skipped already executed local momentum candidate %s", local_id)
                continue

            payload = self._payload(candidate, raw_candidate=raw_candidate, remote_id=remote_id)
            row = self.trade_candidates.upsert_open_candidate(
                candidate_id=local_id,
                symbol=symbol,
                side=str(candidate.get("side") or "long").lower(),
                stage="momentum",
                score=self._float(candidate.get("score"), default=0.0) or 0.0,
                entry_price=self._float(candidate.get("entry_price")),
                stop_price=self._float(candidate.get("stop_price")),
                target_price=self._float(candidate.get("target_price")),
                rr_ratio=self._float(candidate.get("rr_ratio")),
                execution_target=self._dict_or_none(candidate.get("execution_target")),
                liquidity_context=self._dict_or_none(candidate.get("liquidity_context")),
                notes=candidate.get("notes") if isinstance(candidate.get("notes"), str) else None,
                payload=payload,
            )
            summary["upserted"] += 1
            logger.info("Upserted momentum trade candidate %s from remote %s", row.candidate_id, remote_id)

        return summary

    def _url(self, momentum_config: dict[str, Any]) -> str:
        base_url = str(momentum_config.get("signalmaker_base_url") or settings.signalmaker_base_url)
        path = str(momentum_config.get("momentum_candidates_source_path") or settings.momentum_candidates_source_path or "/api/v1/momentum")
        if not path.startswith("/"):
            path = "/" + path
        base = base_url.rstrip("/")
        if base.endswith("/api/v1") and path.startswith("/api/v1"):
            return f"{base}{path[len('/api/v1'):]}"
        return f"{base}{path}"

    def _params(
        self,
        momentum_config: dict[str, Any],
        *,
        limit: int | None,
        min_momentum_score: float | None,
        min_rr: float | None,
        require_wyckoff_context: bool | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "limit": limit if limit is not None else int(momentum_config.get("momentum_candidates_limit", settings.momentum_candidates_limit)),
            "min_momentum_score": min_momentum_score
            if min_momentum_score is not None
            else self._float(
                momentum_config.get("momentum_candidates_min_score"),
                default=settings.momentum_candidates_min_score,
            ),
            "require_wyckoff_context": require_wyckoff_context
            if require_wyckoff_context is not None
            else bool(
                momentum_config.get(
                    "momentum_candidates_require_wyckoff_context",
                    settings.momentum_candidates_require_wyckoff_context,
                )
            ),
        }
        effective_min_rr = min_rr if min_rr is not None else self._float(momentum_config.get("momentum_candidates_min_rr"))
        if effective_min_rr is not None:
            params["min_rr"] = effective_min_rr
        return params

    def _candidate_rows(self, data: Any) -> list[dict[str, Any]]:
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            rows = data.get("items") or data.get("candidates") or data.get("results") or data.get("rankings") or data.get("data") or []
        else:
            rows = []
        return [row for row in rows if isinstance(row, dict)]

    def _as_trade_candidate(self, row: dict[str, Any], momentum_config: dict[str, Any]) -> dict[str, Any]:
        candidate = deepcopy(row)
        price = self._float(candidate.get("entry_price"), default=self._float(candidate.get("price")))
        target = self._float(candidate.get("target_price"), default=self._float(candidate.get("take_profit_price")))
        if target is None and price is not None:
            target_pct = self._float(momentum_config.get("momentum_candidates_target_pct"), default=settings.momentum_candidates_target_pct) or 0.0
            if target_pct > 0:
                target = price * (1.0 + (target_pct / 100.0))
        candidate["candidate_id"] = str(candidate.get("candidate_id") or f"momentum-{str(candidate.get('symbol') or '').upper()}-rank-{candidate.get('rank') or 'open'}")
        candidate["symbol"] = str(candidate.get("symbol") or "").upper().strip()
        candidate["side"] = str(candidate.get("side") or "long").lower()
        candidate["stage"] = "momentum"
        candidate["status"] = str(candidate.get("status") or "open").lower()
        candidate["score"] = self._float(candidate.get("score"), default=self._float(candidate.get("momentum_score"), default=0.0)) or 0.0
        candidate["entry_price"] = price
        candidate["target_price"] = target
        candidate.setdefault("stop_price", None)
        candidate.setdefault("rr_ratio", self._derived_rr(price, self._float(candidate.get("stop_price")), target))
        candidate["liquidity_context"] = self._dict_or_none(candidate.get("liquidity_context")) or {
            "type": "momentum_ranking",
            "rank": candidate.get("rank"),
            "classification": candidate.get("classification"),
            "data_quality": candidate.get("data_quality"),
        }
        candidate.setdefault("execution_target", {"source": "momentum_ranking"})
        candidate.setdefault("notes", self._notes(candidate))
        return candidate

    def _derived_rr(self, entry: float | None, stop: float | None, target: float | None) -> float | None:
        if entry is None or target is None or stop is None or entry == stop:
            return None
        risk = abs(entry - stop)
        if risk <= 0:
            return None
        return abs(target - entry) / risk

    def _notes(self, candidate: dict[str, Any]) -> str:
        rank = candidate.get("rank")
        score = candidate.get("momentum_score") if candidate.get("momentum_score") is not None else candidate.get("score")
        classification = candidate.get("classification") or "momentum"
        return f"Momentum ranking #{rank or '-'} · score={score} · {classification}"

    def _validation_error(self, candidate: dict[str, Any], *, min_score: float, min_rr: float | None) -> str | None:
        symbol = str(candidate.get("symbol") or "").strip()
        if not symbol:
            return "missing_symbol"
        status = str(candidate.get("status") or "open").lower()
        if status not in {"momentum_ready", "open"}:
            return f"unsupported_status:{status or 'missing'}"
        score = self._float(candidate.get("score"), default=0.0) or 0.0
        if score < min_score:
            return f"score_below_min:{score}<{min_score}"
        rr = self._float(candidate.get("rr_ratio"))
        if min_rr is not None and rr is not None and rr < min_rr:
            return f"rr_below_min:{rr}<{min_rr}"
        entry = self._float(candidate.get("entry_price"))
        target = self._float(candidate.get("target_price"))
        if entry is None or target is None:
            return "missing_entry_or_target"
        side = str(candidate.get("side") or "long").lower()
        if side in {"short", "sell", "bear"}:
            if not target < entry:
                return "incoherent_short_entry_target"
        elif not entry < target:
            return "incoherent_long_entry_target"
        return None

    def _payload(self, candidate: dict[str, Any], *, raw_candidate: dict[str, Any], remote_id: str) -> dict[str, Any]:
        payload = deepcopy(candidate.get("payload")) if isinstance(candidate.get("payload"), dict) else {}
        payload["source"] = "momentum_candidates" if raw_candidate.get("candidate_id") or raw_candidate.get("entry_price") is not None else "momentum_rankings"
        payload["remote_candidate_id"] = remote_id
        payload["remote_candidate"] = deepcopy(raw_candidate)
        payload["momentum_trade_candidate"] = deepcopy(candidate)
        return payload

    def _local_candidate_id(self, symbol: str) -> str:
        return f"momentum-{symbol.upper()}-open"

    def _effective_min_score(self, momentum_config: dict[str, Any], min_momentum_score: float | None) -> float:
        return self._float(
            min_momentum_score if min_momentum_score is not None else momentum_config.get("momentum_candidates_min_score"),
            default=settings.momentum_candidates_min_score,
        ) or 0.0

    def _float(self, value: Any, *, default: float | None = None) -> float | None:
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _dict_or_none(self, value: Any) -> dict | None:
        return value if isinstance(value, dict) else None

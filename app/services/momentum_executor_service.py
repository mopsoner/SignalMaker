from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import requests
from sqlalchemy.orm import Session

from app.core.config import settings
from app.services.binance_trading_service import BinanceTradingService
from app.services.fill_service import FillService
from app.services.order_service import OrderService
from app.services.position_service import PositionService
from app.services.runtime_settings import load_runtime_settings


class MomentumExecutorService:
    """Raspberry executor bridge for remote momentum rotation decisions."""

    REQUIRED_DECISION_KEYS = {"action", "symbol", "buy_symbol", "sell_symbol", "reason", "target_asset", "current_position"}

    def __init__(self, db: Session) -> None:
        self.db = db
        self.binance = BinanceTradingService()
        self.positions = PositionService(db)
        self.orders = OrderService(db)
        self.fills = FillService(db)

    def config(self) -> dict[str, Any]:
        payload = load_runtime_settings(self.db).get("momentum", {})
        api_base = str(payload.get("momentum_executor_api_base", settings.momentum_executor_api_base) or settings.momentum_executor_api_base).strip()
        return {
            "momentum_executor_enabled": bool(payload.get("momentum_executor_enabled", settings.momentum_executor_enabled)),
            "momentum_executor_mode": str(payload.get("momentum_executor_mode", settings.momentum_executor_mode) or "paper"),
            "momentum_executor_interval_sec": int(payload.get("momentum_executor_interval_sec", settings.momentum_executor_interval_sec) or 30),
            "momentum_executor_api_base": api_base,
            "momentum_executor_decision_path": str(payload.get("momentum_executor_decision_path", settings.momentum_executor_decision_path) or settings.momentum_executor_decision_path),
            "momentum_executor_quote_asset": str(payload.get("momentum_executor_quote_asset", settings.momentum_executor_quote_asset) or "USDC"),
            "momentum_executor_notional": float(payload.get("momentum_executor_notional", settings.momentum_executor_notional) or 25.0),
            "momentum_executor_apply_remote_run": bool(payload.get("momentum_executor_apply_remote_run", settings.momentum_executor_apply_remote_run)),
        }

    def _api_url(self, path: str) -> str:
        cfg = self.config()
        base = cfg["momentum_executor_api_base"].rstrip("/")
        if not path.startswith("/"):
            path = "/" + path
        if base.endswith("/api/v1") and path.startswith("/api/v1"):
            return base + path[len("/api/v1"):]
        return base + path

    def decision_url(self) -> str:
        return self._api_url(self.config()["momentum_executor_decision_path"])

    def candidates_url(self) -> str:
        return self._api_url("/api/v1/momentum")

    def _json_payload(self, response: requests.Response) -> Any:
        content_type = response.headers.get("content-type", "")
        try:
            return response.json()
        except ValueError as exc:
            body = response.text[:500]
            raise RuntimeError(f"Remote momentum endpoint did not return JSON. status={response.status_code} content_type={content_type} body={body!r}") from exc

    def _response_payload(self, response: requests.Response) -> dict[str, Any]:
        payload = self._json_payload(response)
        if not isinstance(payload, dict):
            raise RuntimeError(f"Remote momentum endpoint returned non-object JSON: {type(payload).__name__}")
        return payload

    def _normalize_decision(self, payload: dict[str, Any]) -> dict[str, Any]:
        if "detail" in payload and "action" not in payload:
            raise RuntimeError(f"Remote momentum endpoint returned error detail: {payload.get('detail')}")
        payload = dict(payload)
        contract = payload.get("executor_contract")
        if isinstance(contract, dict):
            for key in ("action", "raw_action", "symbol", "buy_symbol", "sell_symbol", "reason", "should_trade", "order_sequence"):
                payload.setdefault(key, contract.get(key))
        payload.setdefault("action", "WAIT")
        payload["action"] = str(payload.get("action") or "WAIT").upper()
        payload.setdefault("symbol", payload.get("buy_symbol") or payload.get("sell_symbol"))
        payload.setdefault("buy_symbol", payload.get("symbol") if payload["action"] in {"BUY", "ROTATE"} else None)
        payload.setdefault("sell_symbol", payload.get("symbol") if payload["action"] == "SELL" else None)
        payload.setdefault("reason", payload.get("recommendation") or payload.get("message") or "")
        payload.setdefault("target_asset", None)
        payload.setdefault("current_position", None)
        payload.setdefault("should_trade", payload["action"] in {"BUY", "SELL", "ROTATE"})
        payload.setdefault("mode", "momentum_rotation")
        payload.setdefault("read_at", datetime.now(timezone.utc).isoformat())
        return payload

    def read_decision(self) -> dict[str, Any]:
        response = requests.get(self.decision_url(), timeout=20, headers={"accept": "application/json", "cache-control": "no-cache"})
        payload = self._response_payload(response)
        if not response.ok:
            raise RuntimeError(f"Remote momentum endpoint error HTTP {response.status_code}: {payload}")
        return self._normalize_decision(payload)

    def _extract_candidates(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            raw_candidates = payload
        elif isinstance(payload, dict):
            raw_candidates = payload.get("candidates") or payload.get("items") or payload.get("data") or payload.get("rankings") or []
        else:
            raw_candidates = []
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in raw_candidates:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or row.get("buy_symbol") or "").upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            rows.append({**row, "symbol": symbol, "source": row.get("source") or "momentum_rankings_endpoint"})
        return rows

    def read_candidates(self, *, limit: int = 50) -> list[dict[str, Any]]:
        response = requests.get(self.candidates_url(), params={"limit": limit}, timeout=20, headers={"accept": "application/json", "cache-control": "no-cache"})
        payload = self._json_payload(response)
        if not response.ok:
            raise RuntimeError(f"Remote momentum endpoint error HTTP {response.status_code}: {payload}")
        return self._extract_candidates(payload)

    def current_momentum_position(self):
        rows = self.positions.list_positions(limit=100, status="open")
        for row in rows:
            meta = row.meta or {}
            if meta.get("mode") in {"momentum_paper", "momentum_live"} or meta.get("strategy") == "momentum_rotation":
                return row
        return None

    def status(self) -> dict[str, Any]:
        cfg = self.config()
        try:
            decision = self.read_decision()
        except Exception as exc:
            decision = {"action": "ERROR", "reason": str(exc), "url": self.decision_url()}
        try:
            candidates = self.read_candidates(limit=25)
        except Exception as exc:
            candidates = [{"error": str(exc), "url": self.candidates_url()}]
        position = self.current_momentum_position()
        return {
            "enabled": cfg["momentum_executor_enabled"],
            "mode": cfg["momentum_executor_mode"],
            "api_base": cfg["momentum_executor_api_base"],
            "config": cfg,
            "decision": decision,
            "candidates": candidates,
            "local_position": self._position_payload(position),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def run_once(self, *, force: bool = False) -> dict[str, Any]:
        cfg = self.config()
        if not cfg["momentum_executor_enabled"] and not force:
            return {"enabled": False, "action": "DISABLED", "reason": "momentum_executor_enabled=false"}
        decision = self.read_decision()
        action = str(decision.get("action") or "WAIT").upper()
        mode = cfg["momentum_executor_mode"].lower()
        local_position = self.current_momentum_position()

        if action == "WAIT":
            return {"action": "WAIT", "decision": decision, "local_position": self._position_payload(local_position)}
        if action == "HOLD":
            return {"action": "HOLD", "decision": decision, "local_position": self._position_payload(local_position)}
        if action == "SELL":
            if not self._decision_matches_position(decision, local_position):
                return {"action": "BLOCKED", "decision": decision, "reason": "sell_decision_does_not_match_held_momentum_asset", "local_position": self._position_payload(local_position)}
            sold = self._sell_local_position(local_position, mode=mode, reason=decision.get("reason") or "momentum_sell")
            return {"action": "SELL", "decision": decision, "sold": sold}
        if action == "BUY":
            if local_position:
                return {"action": "HOLD", "decision": decision, "reason": "momentum_position_already_open", "local_position": self._position_payload(local_position)}
            bought = self._buy_best_available(decision, mode=mode, reason=decision.get("reason") or "momentum_buy", cfg=cfg)
            return {"action": "BUY", "decision": decision, "bought": bought}
        if action == "ROTATE":
            if not self._decision_matches_position(decision, local_position):
                return {"action": "BLOCKED", "decision": decision, "reason": "rotate_decision_does_not_match_held_momentum_asset", "local_position": self._position_payload(local_position)}
            sold_symbol = str(local_position.symbol).upper()
            sold = self._sell_local_position(local_position, mode=mode, reason=decision.get("reason") or "momentum_rotate_sell")
            bought = self._buy_best_available(decision, mode=mode, reason=decision.get("reason") or "momentum_rotate_buy", cfg=cfg, exclude={sold_symbol})
            return {"action": "ROTATE", "decision": decision, "sold": sold, "bought": bought}
        if action == "ERROR":
            return {"action": "ERROR", "decision": decision, "reason": decision.get("reason")}
        return {"action": "UNKNOWN", "decision": decision, "reason": f"Unsupported action {action}"}

    def _decision_matches_position(self, decision: dict[str, Any], position) -> bool:
        if not position:
            return False
        held = str(position.symbol or "").upper()
        decision_symbols = {
            str(decision.get("sell_symbol") or "").upper(),
            str(decision.get("symbol") or "").upper(),
        }
        return held in decision_symbols

    def _decision_candidates(self, decision: dict[str, Any], *, exclude: set[str] | None = None) -> list[dict[str, Any]]:
        exclude = {item.upper() for item in (exclude or set()) if item}
        rows: list[dict[str, Any]] = []
        try:
            rows.extend(self.read_candidates(limit=50))
        except Exception as exc:
            rows.append({"error": str(exc), "source": "momentum_rankings_endpoint"})
        contract = decision.get("executor_contract") if isinstance(decision.get("executor_contract"), dict) else {}
        rows.extend(self._extract_candidates(decision.get("buy_candidates") or contract.get("buy_candidates") or []))
        primary = str(decision.get("buy_symbol") or decision.get("symbol") or "").upper()
        if primary:
            rows.append({"symbol": primary, "source": "decision_primary"})
        seen: set[str] = set()
        candidates: list[dict[str, Any]] = []
        for row in rows:
            symbol = str(row.get("symbol") or row.get("buy_symbol") or "").upper() if isinstance(row, dict) else ""
            if not symbol or symbol in seen or symbol in exclude:
                continue
            seen.add(symbol)
            candidates.append({**row, "symbol": symbol})
        return candidates

    def _buy_result_ok(self, result: dict[str, Any]) -> bool:
        return bool(result) and not result.get("skipped") and bool(result.get("position_id"))

    def _buy_best_available(self, decision: dict[str, Any], *, mode: str, reason: str, cfg: dict[str, Any], exclude: set[str] | None = None) -> dict[str, Any]:
        attempts: list[dict[str, Any]] = []
        candidates = self._decision_candidates(decision, exclude=exclude)
        if not candidates:
            return {"skipped": True, "reason": "no_momentum_rankings", "attempts": attempts}
        for row in candidates:
            symbol = str(row.get("symbol") or "").upper()
            try:
                result = self._buy_symbol(symbol, mode=mode, reason=reason, cfg=cfg)
                attempts.append({"symbol": symbol, "result": result, "rank": row.get("rank"), "score": row.get("momentum_score"), "source": row.get("source")})
                if self._buy_result_ok(result):
                    return {"selected_symbol": symbol, "result": result, "attempts": attempts}
            except Exception as exc:
                attempts.append({"symbol": symbol, "error": str(exc), "rank": row.get("rank"), "score": row.get("momentum_score"), "source": row.get("source")})
        return {"skipped": True, "reason": "buy_not_confirmed_for_any_momentum_candidate", "attempts": attempts}

    def _buy_symbol(self, symbol: str, *, mode: str, reason: str, cfg: dict[str, Any]) -> dict[str, Any]:
        symbol = symbol.upper()
        if not symbol or symbol == "NONE":
            return {"skipped": True, "reason": "missing_symbol"}
        price = self.binance.current_price(symbol)
        notional = float(cfg["momentum_executor_notional"])
        quantity = notional / price if price else 0.0
        if mode == "live":
            if not settings.live_trading_enabled:
                raise RuntimeError("LIVE_TRADING_ENABLED=false")
            normalized = self.binance.normalize_order(symbol, quantity=quantity, target_price=None, stop_price=None)
            order_payload = self.binance.place_market_buy(symbol, normalized["quantity"])
            filled_qty = float(order_payload.get("executedQty") or normalized["quantity"])
            avg_price = self.binance.average_fill_price(order_payload) or price
            mode_meta = "momentum_live"
            exchange = order_payload
        else:
            filled_qty = quantity
            avg_price = price
            mode_meta = "momentum_paper"
            exchange = None

        position = self.positions.create_position(
            symbol=symbol,
            side="long",
            quantity=filled_qty,
            entry_price=avg_price,
            mark_price=avg_price,
            stop_price=None,
            target_price=None,
            meta={"mode": mode_meta, "strategy": "momentum_rotation", "reason": reason},
        )
        order = self.orders.create_order(
            candidate_id=None,
            position_id=position.position_id,
            symbol=symbol,
            side="buy",
            order_type="market",
            quantity=filled_qty,
            requested_price=price,
            filled_price=avg_price,
            status="filled",
            meta={"mode": mode_meta, "strategy": "momentum_rotation", "exchange": exchange},
        )
        fill = self.fills.create_fill(order_id=order.order_id, position_id=position.position_id, symbol=symbol, side="buy", quantity=filled_qty, price=avg_price)
        return {"symbol": symbol, "position_id": position.position_id, "order_id": order.order_id, "fill_id": fill.fill_id, "quantity": filled_qty, "price": avg_price, "mode": mode_meta}

    def _sell_local_position(self, position, *, mode: str, reason: str) -> dict[str, Any]:
        if not position:
            return {"skipped": True, "reason": "no_local_momentum_position"}
        symbol = position.symbol
        quantity = float(position.quantity or 0.0)
        price = self.binance.current_price(symbol)
        if mode == "live":
            if not settings.live_trading_enabled:
                raise RuntimeError("LIVE_TRADING_ENABLED=false")
            order_payload = self.binance.place_market_sell(symbol, quantity)
            avg_price = self.binance.average_fill_price(order_payload) or price
            mode_meta = "momentum_live"
            exchange = order_payload
        else:
            avg_price = price
            mode_meta = "momentum_paper"
            exchange = None
        pnl = (avg_price - float(position.entry_price or avg_price)) * quantity
        self.positions.close_position(position.position_id, mark_price=avg_price, unrealized_pnl=pnl)
        order = self.orders.create_order(candidate_id=None, position_id=position.position_id, symbol=symbol, side="sell", order_type="market", quantity=quantity, requested_price=price, filled_price=avg_price, status="filled", meta={"mode": mode_meta, "strategy": "momentum_rotation", "reason": reason, "exchange": exchange})
        fill = self.fills.create_fill(order_id=order.order_id, position_id=position.position_id, symbol=symbol, side="sell", quantity=quantity, price=avg_price)
        return {"symbol": symbol, "position_id": position.position_id, "order_id": order.order_id, "fill_id": fill.fill_id, "quantity": quantity, "price": avg_price, "pnl": pnl, "mode": mode_meta}

    def _position_payload(self, position) -> dict[str, Any] | None:
        if not position:
            return None
        return {
            "position_id": position.position_id,
            "symbol": position.symbol,
            "status": position.status,
            "side": position.side,
            "quantity": position.quantity,
            "entry_price": position.entry_price,
            "mark_price": position.mark_price,
            "unrealized_pnl": position.unrealized_pnl,
            "opened_at": position.opened_at,
            "meta": position.meta,
        }

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DATABASE_URL", "sqlite:///./gateway_smoke.db")
os.environ.setdefault("CREATE_TABLES_ON_BOOT", "true")

from app.db.base import init_db
from app.db.session import SessionLocal
from app.models.trade_candidate import TradeCandidate
from app.schemas.gateway import GatewayExecutionEvent, GatewayExecutionReport
from app.services.gateway_execution_service import GatewayExecutionService


def main() -> None:
    db_path = ROOT / "gateway_smoke.db"
    if db_path.exists():
        db_path.unlink()

    init_db()
    db = SessionLocal()
    try:
        candidate = TradeCandidate(
            candidate_id="SMOKE-ETHUSDT-open",
            symbol="ETHUSDT",
            side="long",
            stage="smoke",
            status="open",
            score=10,
            entry_price=3000.0,
            stop_price=2950.0,
            target_price=3100.0,
            rr_ratio=2.0,
            payload={"smoke": True},
        )
        db.add(candidate)
        db.commit()

        service = GatewayExecutionService(db)
        execution = GatewayExecutionReport(
            gateway_id="smoke-raspberry",
            candidate_id="SMOKE-ETHUSDT-open",
            signal_symbol="ETHUSDT",
            execution_symbol="ETHUSDC",
            side="long",
            quantity=0.01,
            entry_price=3001.0,
            stop_price=2950.0,
            target_price=3100.0,
            mode="dry_run",
            entry_order={"exchange_order_id": "entry-1", "status": "FILLED", "avg_price": 3001.0, "executed_qty": 0.01},
            tp_order={"exchange_order_id": "tp-1", "status": "NEW", "price": 3100.0},
            sl_order={"exchange_order_id": "sl-1", "status": "NEW", "price": 2950.0},
        )
        result = service.record_execution(execution)
        assert result["status"] == "recorded"

        event = GatewayExecutionEvent(
            gateway_id="smoke-raspberry",
            candidate_id="SMOKE-ETHUSDT-open",
            event_type="take_profit_filled",
            exchange_order_id="tp-1",
        )
        event_result = service.record_event(event)
        assert event_result["status"] == "recorded"
        print("gateway smoke test ok", result, event_result)
    finally:
        db.close()


if __name__ == "__main__":
    main()

from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models.base import Base
from app.models.momentum_structure_current import MomentumStructureCurrent
from app.services.momentum_service import MomentumService


def _structure_payload(status: str) -> dict:
    return {
        "structure_15m_status": status,
        "structure_15m_bias": "neutral_bullish",
        "mss_15m_bearish": False,
        "bos_15m_bearish": False,
        "bos_15m_bullish": False,
        "last_swing_low_15m": 100.0,
        "last_swing_high_15m": 110.0,
        "structure_broken_at": None,
        "structure_reason": "test",
    }


def test_structure_upsert_uses_complete_scoped_primary_key() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)

    with Session(engine) as db:
        service = MomentumService(db)
        calculated_at = datetime.now(timezone.utc)

        service._upsert_structure("BTCUSD", _structure_payload("valid"), calculated_at=calculated_at)
        db.flush()
        service._upsert_structure("BTCUSD", _structure_payload("valid_bullish"), calculated_at=calculated_at)
        db.flush()

        row = db.get(MomentumStructureCurrent, ("BTCUSD", "crypto"))
        assert row is not None
        assert row.market_scope == "crypto"
        assert row.structure_15m_status == "valid_bullish"


def test_structure_map_does_not_mix_market_scopes() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)

    with Session(engine) as db:
        db.add_all([
            MomentumStructureCurrent(symbol="SHARED", market_scope="crypto", structure_15m_status="valid"),
            MomentumStructureCurrent(symbol="SHARED", market_scope="stock_etf", structure_15m_status="broken_bearish"),
        ])
        db.commit()

        rows = MomentumService(db)._structure_map(["SHARED"])

        assert rows["SHARED"].market_scope == "crypto"
        assert rows["SHARED"].structure_15m_status == "valid"

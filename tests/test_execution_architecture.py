from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.models.base import Base
from app.models.candidate_execution import CandidateExecution
from app.models.trade_candidate import TradeCandidate
from app.services.trade_candidate_service import TradeCandidateService


def test_execution_layer_has_no_raspberry_or_internal_http_dependency():
    root = Path("app/services/execution")
    source = "\n".join(path.read_text() for path in root.glob("*.py"))
    for forbidden in ("StateStore", "SignalMakerClient", "raspberry_executor", "/api/v1/market-data/candles", "codex_reference"):
        assert forbidden not in source


def _candidate() -> TradeCandidate:
    return TradeCandidate(
        candidate_id="BTCUSD-signal", symbol="BTCUSD", side="bull", stage="spring",
        status="open", score=90, entry_price=100, stop_price=90, target_price=120,
    )


def test_claim_is_atomic_between_workers_of_the_same_mode(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'claims.db'}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add(_candidate())
        db.commit()

    def claim() -> list[str]:
        with Session(engine) as db:
            return [row.candidate_id for row in TradeCandidateService(db).claim_open_candidates(execution_mode="paper")]

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _index: claim(), range(2)))

    assert sorted(len(result) for result in results) == [0, 1]
    with Session(engine) as db:
        assert len(list(db.scalars(select(CandidateExecution)).all())) == 1


def test_claims_are_isolated_by_execution_mode(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'modes.db'}")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add(_candidate())
        db.commit()
        repository = TradeCandidateService(db)
        assert [row.candidate_id for row in repository.claim_open_candidates(execution_mode="paper")] == ["BTCUSD-signal"]
        assert [row.candidate_id for row in repository.claim_open_candidates(execution_mode="live")] == ["BTCUSD-signal"]
        assert repository.claim_open_candidates(execution_mode="paper") == []
        repository.finish_execution("BTCUSD-signal", execution_mode="paper")
        states = {
            row.execution_mode: row.status
            for row in db.scalars(select(CandidateExecution)).all()
        }
        assert states == {"paper": "completed", "live": "claimed"}

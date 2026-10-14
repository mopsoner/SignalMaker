from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models.base import Base
from app.models.trade_candidate import TradeCandidate
from app.models.candidate_execution import CandidateExecution
from app.services.trade_candidate_service import TradeCandidateService


@pytest.fixture
def service():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add(TradeCandidate(candidate_id="FETUSD-open", symbol="FETUSD", side="bull", stage="spring", status="open", score=1))
        db.commit()
        yield TradeCandidateService(db)


def state(service, **values):
    row = CandidateExecution(execution_id="FETUSD-open-live", candidate_id="FETUSD-open", execution_mode="live", **values)
    service.db.add(row)
    service.db.commit()
    return row


def test_manual_retry_reclaims_once_after_pre_submission_rejection(service):
    state(service, status="retryable", error="insufficient balance", next_attempt_at=datetime.now(timezone.utc))
    assert service.claim_candidate("FETUSD-open", execution_mode="live") is not None
    assert service.claim_candidate("FETUSD-open", execution_mode="live") is None


@pytest.mark.parametrize("values", [
    {"status": "completed"},
    {"status": "retryable", "entry_order_id": "kraken-order"},
    {"status": "failed", "error": "request timed out"},
    {"status": "retryable", "next_attempt_at": datetime.now(timezone.utc) + timedelta(hours=1)},
])
def test_manual_retry_preserves_duplicate_and_ambiguous_submission_guards(service, values):
    state(service, **values)
    assert service.claim_candidate("FETUSD-open", execution_mode="live") is None


def test_legacy_known_preflight_failure_can_be_retried(service):
    state(service, status="failed", error="bearish Wyckoff candidates require margin mode")
    assert service.claim_candidate("FETUSD-open", execution_mode="live") is not None


def test_failed_execution_displays_original_error(service):
    state(service, status="failed", error="Kraken timeout")
    assert "Kraken timeout" in service.claim_failure_reason("FETUSD-open", execution_mode="live")

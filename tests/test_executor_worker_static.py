from pathlib import Path


WORKER_SOURCE = Path(__file__).resolve().parents[1] / "scripts" / "run_executor_loop.py"


def test_executor_worker_does_not_sync_momentum_candidates():
    source = WORKER_SOURCE.read_text()

    assert "MomentumCandidateSyncService" not in source
    assert "momentum_candidates_sync_enabled" not in source
    assert "sync_momentum_first" not in source


def test_executor_worker_uses_separate_momentum_decision_flow():
    source = WORKER_SOURCE.read_text()

    assert 'bot_executor_momentum_enabled' in source
    assert 'execute_momentum_decision' in source
    assert 'execute_open_candidates(limit=limit, quantity=quantity, mode=mode)' in source

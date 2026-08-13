from types import SimpleNamespace

import pytest

from scripts import run_momentum_executor_loop as worker


def test_live_worker_rejects_dry_run(monkeypatch):
    monkeypatch.setattr(
        worker,
        "settings",
        SimpleNamespace(
            momentum_execution_enabled=True,
            kraken_execution_enabled=True,
            kraken_dry_run=True,
            kraken_api_key="key",
            kraken_secret_key="secret",
            momentum_execution_mode="spot",
            kraken_margin_execution_enabled=False,
        ),
    )

    with pytest.raises(RuntimeError, match="KRAKEN_DRY_RUN must be false"):
        worker.assert_live_configuration()


def test_live_worker_accepts_complete_spot_configuration(monkeypatch):
    monkeypatch.setattr(
        worker,
        "settings",
        SimpleNamespace(
            momentum_execution_enabled=True,
            kraken_execution_enabled=True,
            kraken_dry_run=False,
            kraken_api_key="key",
            kraken_secret_key="secret",
            momentum_execution_mode="spot",
            kraken_margin_execution_enabled=False,
        ),
    )

    worker.assert_live_configuration()

from types import SimpleNamespace

import pytest

from scripts import run_wyckoff_live_loop as worker


def configuration(**overrides):
    values = {
        "wyckoff_live_enabled": True,
        "wyckoff_live_mode": "spot",
        "kraken_execution_enabled": True,
        "kraken_dry_run": False,
        "kraken_api_key": "key",
        "kraken_secret_key": "secret",
        "kraken_margin_execution_enabled": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_live_worker_rejects_dry_run(monkeypatch):
    monkeypatch.setattr(worker, "settings", configuration(kraken_dry_run=True))

    with pytest.raises(RuntimeError, match="KRAKEN_DRY_RUN must be false"):
        worker.assert_live_configuration()


def test_live_worker_accepts_complete_spot_configuration(monkeypatch):
    monkeypatch.setattr(worker, "settings", configuration())

    worker.assert_live_configuration()


def test_live_worker_requires_margin_guard_for_margin_mode(monkeypatch):
    monkeypatch.setattr(worker, "settings", configuration(wyckoff_live_mode="margin"))

    with pytest.raises(RuntimeError, match="KRAKEN_MARGIN_EXECUTION_ENABLED must be true"):
        worker.assert_live_configuration()

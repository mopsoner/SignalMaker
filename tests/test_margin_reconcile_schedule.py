from raspberry_executor import run_all_v2


def test_margin_reconcile_interval_defaults_to_five_minutes(monkeypatch):
    monkeypatch.delenv("KRAKEN_MARGIN_RECONCILE_SECONDS", raising=False)
    assert run_all_v2.margin_reconcile_interval_seconds() == 300


def test_margin_reconcile_interval_has_safe_minimum(monkeypatch):
    monkeypatch.setenv("KRAKEN_MARGIN_RECONCILE_SECONDS", "5")
    assert run_all_v2.margin_reconcile_interval_seconds() == 60

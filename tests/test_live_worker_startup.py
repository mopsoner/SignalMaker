from types import SimpleNamespace
from unittest.mock import Mock

from app.core.config import Settings
from app.services.live_worker_startup import start_live_workers, stop_started_workers


def configuration(**values):
    return SimpleNamespace(**{
        "wyckoff_live_enabled": True, "kraken_profit_worker_enabled": True,
        "wyckoff_live_mode": "spot", "kraken_execution_enabled": True,
        "kraken_dry_run": False, "kraken_api_key": "test-key", "kraken_secret_key": "test-secret",
        "kraken_margin_execution_enabled": False, **values})


def controller():
    result = Mock(supervisor="local")
    result.start.return_value = {"action": "started"}
    return result


def test_both_default_enabled_and_env_override(tmp_path):
    defaults = Settings(_env_file=None)
    assert defaults.wyckoff_live_enabled and defaults.kraken_profit_worker_enabled
    env = tmp_path / ".env"
    env.write_text("WYCKOFF_LIVE_ENABLED=false\nKRAKEN_PROFIT_WORKER_ENABLED=false\n")
    disabled = Settings(_env_file=env)
    assert not disabled.wyckoff_live_enabled and not disabled.kraken_profit_worker_enabled


def test_enabled_workers_start_and_owned_workers_stop():
    control = controller()
    _, started = start_live_workers(configuration(), control)
    assert [c.args[0] for c in control.start.call_args_list] == ["wyckoff_live", "kraken_profit"]
    stop_started_workers(control, started)
    assert [c.args[0] for c in control.stop.call_args_list] == started


def test_disabled_workers_are_stopped_without_starting():
    control = controller()
    start_live_workers(configuration(wyckoff_live_enabled=False, kraken_profit_worker_enabled=False), control)
    control.start.assert_not_called()
    assert control.stop.call_count == 2


def test_incomplete_kraken_configuration_never_starts_real_workers():
    control = controller()
    start_live_workers(configuration(kraken_dry_run=True), control)
    control.start.assert_not_called()


def test_one_failure_does_not_block_other_worker():
    control = controller()
    control.start.side_effect = [RuntimeError("start failed"), {"action": "started"}]
    _, started = start_live_workers(configuration(), control)
    assert started == ["kraken_profit"]


def test_existing_workers_and_systemd_workers_are_not_stopped_on_api_shutdown():
    for supervisor, action in (("local", "noop"), ("systemd", "started")):
        control = controller()
        control.supervisor = supervisor
        control.start.return_value = {"action": action}
        _, started = start_live_workers(configuration(), control)
        stop_started_workers(control, started)
        assert started == []
        control.stop.assert_not_called()

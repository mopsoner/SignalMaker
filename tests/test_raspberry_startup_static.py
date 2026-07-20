from pathlib import Path


def test_raspberry_service_waits_for_network_and_postgresql():
    service = Path("systemd/raspberry-executor.service").read_text()

    dependencies = "network-online.target postgresql.service"
    assert f"After={dependencies}" in service
    assert f"Wants={dependencies}" in service


def test_run_sh_device_checks_postgresql_before_starting_api():
    launcher = Path("run.sh").read_text()

    database_waiter = launcher[
        launcher.index("wait_for_database()") : launcher.index("start_api_and_device()")
    ]
    device_start = launcher.index("start_api_and_device()")
    database_check = launcher.index("wait_for_database", device_start)
    api_start = launcher.index('bash scripts/start_api.sh "$@"', device_start)
    assert database_check < api_start
    assert "pg_isready" in database_waiter

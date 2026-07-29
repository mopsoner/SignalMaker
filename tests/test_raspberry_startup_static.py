from pathlib import Path


INSTALL_SCRIPT = Path("scripts/install_system_start.sh")
UNINSTALL_SCRIPT = Path("scripts/uninstall_system_start.sh")


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


def test_system_start_installs_only_the_executor_service():
    installer = INSTALL_SCRIPT.read_text()

    assert installer.count('cat > "/etc/systemd/system/${EXECUTOR_SERVICE}"') == 1
    assert installer.count("cat >") == 1
    assert installer.count('systemctl enable "${EXECUTOR_SERVICE}"') == 1
    assert "TUI_SERVICE" not in installer
    assert "signalmaker-tui" not in installer
    assert "getty" not in installer


def test_installed_executor_uses_run_sh_default_device_mode():
    installer = INSTALL_SCRIPT.read_text()
    exec_start = next(
        line for line in installer.splitlines() if line.startswith("ExecStart=")
    )

    assert exec_start == "ExecStart=/bin/bash ${APP_DIR}/run.sh"
    assert not exec_start.endswith(" device")


def test_system_start_uninstall_removes_current_and_legacy_services_without_getty():
    uninstaller = UNINSTALL_SCRIPT.read_text()

    assert 'EXECUTOR_SERVICE="raspberry-executor.service"' in uninstaller
    assert 'TUI_SERVICE="signalmaker-tui.service"' in uninstaller
    assert 'LEGACY_BOT_SERVICE="signalmaker-bot.service"' in uninstaller
    services = (
        'for service in "$TUI_SERVICE" "$EXECUTOR_SERVICE" "$LEGACY_BOT_SERVICE"'
    )
    assert services in uninstaller
    assert 'systemctl stop "$service"' in uninstaller
    assert 'systemctl disable "$service"' in uninstaller
    assert 'rm -f "/etc/systemd/system/$service"' in uninstaller
    assert "getty" not in uninstaller

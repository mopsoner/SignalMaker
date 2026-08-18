import ast
import re
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
WORKER_CONTROL_SERVICE = ROOT_DIR / "app/services/worker_control_service.py"
RESERVED_VM_SCRIPT = ROOT_DIR / "scripts/start_reserved_vm.sh"
LEGACY_VM_SCRIPT = ROOT_DIR / "start_vm.sh"


def _configured_worker_names() -> set[str]:
    tree = ast.parse(WORKER_CONTROL_SERVICE.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "WORKERS"
            for target in node.targets
        ):
            assert isinstance(node.value, ast.Dict)
            return {
                key.value
                for key in node.value.keys
                if isinstance(key, ast.Constant) and isinstance(key.value, str)
            }
    raise AssertionError("WORKERS configuration was not found")


def test_reserved_vm_script_writes_a_pid_file_for_every_configured_worker() -> None:
    script = RESERVED_VM_SCRIPT.read_text()
    launched_workers = set(re.findall(r'^\s*start_worker "([a-z_]+)" ', script, re.MULTILINE))

    assert launched_workers == _configured_worker_names()
    assert 'printf \'%s\\n\' "$pid" > "$RUNTIME_DIR/$name.pid"' in script
    assert 'rm -f "${WORKER_PID_FILES[@]}"' in script


def test_every_deployment_worker_redirects_both_streams_to_api_log_path() -> None:
    systemd_dir = ROOT_DIR / "deploy/systemd"
    unit_by_worker = {
        "pipeline": "signalmaker-pipeline.service",
        "wyckoff_paper": "signalmaker-wyckoff-paper.service",
        "scheduler": "signalmaker-scheduler.service",
        "momentum_paper": "signalmaker-momentum-paper.service",
        "momentum_live": "signalmaker-momentum-live.service",
        "wyckoff_live": "signalmaker-wyckoff-live.service",
        "kraken_candle_feed": "signalmaker-kraken-candle-feed.service",
        "ibkr_ingestion": "signalmaker-ibkr-ingestion.service",
        "stock_etf_analysis": "signalmaker-market-analysis.service",
    }

    assert set(unit_by_worker) == _configured_worker_names()
    for worker, unit_name in unit_by_worker.items():
        unit = (systemd_dir / unit_name).read_text(encoding="utf-8")
        destination = f"append:/opt/signalmaker/logs/{worker}.log"
        assert "Environment=SIGNALMAKER_LOG_DIR=/opt/signalmaker/logs" in unit
        assert f"StandardOutput={destination}" in unit
        assert f"StandardError={destination}" in unit


def test_reserved_vm_redirects_both_streams_to_configured_log_directory() -> None:
    script = RESERVED_VM_SCRIPT.read_text(encoding="utf-8")

    assert 'export SIGNALMAKER_LOG_DIR="${SIGNALMAKER_LOG_DIR:-$APP_DIR/logs}"' in script
    assert 'bash "$script" >> "$LOG_DIR/$name.log" 2>&1 &' in script


def test_legacy_vm_entrypoint_uses_the_shared_log_directory_configuration() -> None:
    script = LEGACY_VM_SCRIPT.read_text(encoding="utf-8")

    assert 'export SIGNALMAKER_LOG_DIR="${SIGNALMAKER_LOG_DIR:-$PWD/logs}"' in script
    for worker in ("pipeline", "wyckoff_paper", "scheduler"):
        assert f'>> "$LOG_DIR/{worker}.log" 2>&1 &' in script

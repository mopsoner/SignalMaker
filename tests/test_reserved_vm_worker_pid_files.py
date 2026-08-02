import ast
import re
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
WORKER_CONTROL_SERVICE = ROOT_DIR / "app/services/worker_control_service.py"
RESERVED_VM_SCRIPT = ROOT_DIR / "scripts/start_reserved_vm.sh"


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
    launched_workers = set(re.findall(r'^start_worker "([a-z_]+)" ', script, re.MULTILINE))

    assert launched_workers == _configured_worker_names()
    assert 'printf \'%s\\n\' "$pid" > "$RUNTIME_DIR/$name.pid"' in script
    assert 'rm -f "${WORKER_PID_FILES[@]}"' in script

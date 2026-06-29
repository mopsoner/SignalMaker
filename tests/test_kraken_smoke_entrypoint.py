import subprocess
import sys


def test_kraken_full_smoke_test_entrypoint_help_runs_without_syntax_error():
    completed = subprocess.run(
        [sys.executable, "raspberry_executor/kraken_full_smoke_test.py", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert "--symbol" in completed.stdout

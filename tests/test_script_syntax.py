import py_compile
from pathlib import Path


def test_kraken_full_smoke_test_has_valid_python_syntax():
    py_compile.compile(str(Path("raspberry_executor/kraken_full_smoke_test.py")), doraise=True)

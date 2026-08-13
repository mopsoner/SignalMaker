from pathlib import Path


def test_execution_layer_has_no_raspberry_or_internal_http_dependency():
    root = Path("app/services/execution")
    source = "\n".join(path.read_text() for path in root.glob("*.py"))
    for forbidden in ("StateStore", "SignalMakerClient", "raspberry_executor", "/api/v1/market-data/candles", "codex_reference"):
        assert forbidden not in source

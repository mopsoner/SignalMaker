from pathlib import Path


def test_application_has_no_removed_provider_imports() -> None:
    roots = (Path("app"), Path("signalmaker"))
    forbidden = "signalmaker.data_providers." + "eod" + "hd"
    offenders = []
    for root in roots:
        for path in root.rglob("*.py"):
            if forbidden in path.read_text():
                offenders.append(str(path))
    assert offenders == []

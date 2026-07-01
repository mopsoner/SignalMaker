from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services import runtime_settings


SUMMARY_FUNCTION_RE = re.compile(
    r"function renderSettingsSummary\(p\)\{(?P<body>.*?)\n  \}\n\n  function loadHealth",
    re.S,
)
PATH_RE = re.compile(r"(?:pick\(p,\['([^']+)'\]\)|hasPath\(p,'([^']+)'\))")


def _summary_paths(source: str) -> set[str]:
    match = SUMMARY_FUNCTION_RE.search(source)
    assert match, "renderSettingsSummary() should be present and followed by loadHealth()."
    paths: set[str] = set()
    for pick_path, has_path in PATH_RE.findall(match.group("body")):
        paths.add(pick_path or has_path)
    return paths


def test_settings_summary_uses_admin_editable_paths() -> None:
    source = Path("frontend/app.js").read_text()
    paths = _summary_paths(source)
    editable_paths = {
        f"{section}.{key}"
        for section, keys in runtime_settings.ADMIN_EDITABLE_FIELDS.items()
        for key in keys
    }

    assert paths <= editable_paths
    assert "momentum.signalmaker_base_url" in paths
    assert "executor.execution_exchange" in paths
    assert "live.live_trading_enabled" in paths
    assert "market_data.kraken_collector_enabled" in paths
    assert {
        "general.signalmaker_base_url",
        "market_data.signalmaker_base_url",
        "executor.signalmaker_base_url",
        "executor.dry_run",
        "live.dry_run",
        "market_data.candle_feed_enabled",
        "executor.candle_feed_enabled",
    }.isdisjoint(paths)


def test_settings_summary_normalizes_string_booleans() -> None:
    source = Path("frontend/app.js").read_text()
    match = SUMMARY_FUNCTION_RE.search(source)
    assert match, "renderSettingsSummary() should be present and followed by loadHealth()."
    body = match.group("body")

    assert "function toBool(value)" in source
    assert "s === 'true' || s === '1'" in source
    assert "s === 'false' || s === '0'" in source
    assert "toBool(pick(p,['live.live_trading_enabled']))" in body
    assert "toBool(pick(p,['market_data.kraken_collector_enabled']))" in body
    assert "candleFeedEnabled===true?'enabled':(candleFeedEnabled===false?'disabled':candleFeedEnabled)" in body


def test_versioned_static_build_matches_frontend_source() -> None:
    assert Path("frontend/dist/app.js").read_text() == Path("frontend/app.js").read_text()

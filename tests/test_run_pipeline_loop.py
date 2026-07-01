import importlib.util
from pathlib import Path


spec = importlib.util.spec_from_file_location(
    "run_pipeline_loop", Path("scripts/run_pipeline_loop.py")
)
run_pipeline_loop = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(run_pipeline_loop)


class FakePipelineService:
    limits = []

    def __init__(self, db):
        self.db = db

    def run_once(self, *, limit):
        self.limits.append(limit)
        return {"ok": True}


def test_pipeline_loop_uses_market_data_kraken_max_symbols(monkeypatch):
    FakePipelineService.limits = []
    monkeypatch.setattr(run_pipeline_loop, "PipelineService", FakePipelineService)
    monkeypatch.setattr(
        run_pipeline_loop,
        "load_runtime_settings",
        lambda db: {
            "bot": {"bot_pipeline_enabled": True, "bot_pipeline_interval_sec": 7},
            "market_data": {"kraken_max_symbols": "11"},
            "kraken": {"kraken_max_symbols": "99"},
        },
    )

    interval = run_pipeline_loop._run_pipeline_tick(object())

    assert interval == 7
    assert FakePipelineService.limits == [11]


def test_pipeline_loop_falls_back_to_legacy_kraken_max_symbols(monkeypatch):
    FakePipelineService.limits = []
    monkeypatch.setattr(run_pipeline_loop, "PipelineService", FakePipelineService)
    monkeypatch.setattr(
        run_pipeline_loop,
        "load_runtime_settings",
        lambda db: {
            "bot": {"bot_pipeline_enabled": True},
            "kraken": {"kraken_max_symbols": "13"},
        },
    )

    interval = run_pipeline_loop._run_pipeline_tick(object())

    assert interval == run_pipeline_loop.DEFAULT_INTERVAL
    assert FakePipelineService.limits == [13]

import asyncio
from types import SimpleNamespace

from signalmaker.jobs.ibkr_run_analysis import build_parser
from signalmaker.market_data.analysis_service import MarketAnalysisService


class Repo:
    def __init__(self):
        self.db = SimpleNamespace(commit=lambda: None, rollback=lambda: None)
        self.rows = []
        self.finished = None
        self.filters = None

    async def create_analysis_run(self, *args, **kwargs): return 41
    async def list_enabled_market_assets(self, **kwargs):
        self.filters = kwargs
        return [{"id": "good", "provider_symbol": "GOOD"}, {"id": "bad", "provider_symbol": "BAD"}]
    async def analysis_result_exists(self, key): return False
    async def insert_analysis_result(self, *args, **kwargs): self.rows.append((args, kwargs))
    async def finish_analysis_run(self, *args): self.finished = args


class Adapter:
    async def load_stock_etf_candle_bundle(self, asset_id, timeframes):
        if asset_id == "bad":
            raise RuntimeError("isolated")
        return {tf: [{"timestamp": 1, "close_time": 2, "open": 1, "high": 1, "low": 1, "close": 1}] for tf in timeframes}

    def normalize_engine_candles(self, rows, **kwargs): return rows

class Pipeline:
    calls = []
    def run(self, **kwargs):
        self.calls.append(kwargs)
        return {"engine_name": "momentum", "status": "completed", "phase": "persisting",
                "missing_timeframes": [], "signal": "HOLD", "payload": {}}


def test_cli_keeps_selection_filters():
    args = build_parser().parse_args(["--engine", "momentum", "--universe", "PEA", "--asset-type", "ETF", "--limit", "7"])
    assert (args.engine, args.universe, args.asset_type, args.limit) == ("momentum", "PEA", "ETF", 7)


def test_shared_orchestration_persists_run_and_isolates_asset_failure():
    repo = Repo()
    pipeline = Pipeline()
    report = asyncio.run(MarketAnalysisService(repo, adapter=Adapter(), pipeline=pipeline).run(
        engine="momentum", universe="PEA", asset_type="ETF", limit=7
    ))
    assert repo.filters["universe_name"] == "PEA"
    assert report["run_id"] == 41
    assert report["summary"] == {"total": 2, "processed": 2, "completed": 1, "insufficient_data": 0, "skipped": 0, "failed": 1}
    assert len(repo.rows) == 1
    assert pipeline.calls[-1]["market_scope"] == "stock_etf"
    assert pipeline.calls[-1]["asset_id"] == "good"
    assert pipeline.calls[-1]["universe"] == "PEA"
    assert pipeline.calls[-1]["asset_type"] == "ETF"
    assert pipeline.calls[-1]["workflow_version"] == MarketAnalysisService.WORKFLOW_VERSION
    assert repo.finished[1] == "PARTIAL"

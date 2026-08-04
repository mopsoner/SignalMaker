"""Guard the single Stock/ETF analysis orchestration boundary."""
from pathlib import Path


ROOT = Path(__file__).parents[1]


def test_every_stock_etf_job_entry_uses_market_analysis_service():
    entry_points = {
        "signalmaker/jobs/ibkr_run_analysis.py": "MarketAnalysisService",
        "signalmaker/market_data/services.py": "MarketAnalysisService",
        "app/api/routes/admin_market_data.py": "MarketAnalysisService",
    }
    for relative, service in entry_points.items():
        source = (ROOT / relative).read_text()
        assert service in source, f"{relative} bypasses the common analytical entry point"


def test_entry_points_do_not_restore_simplified_analysis_heuristics():
    forbidden = ("ma50", "ma200", "return_20", "20_day_return", "simplified_range")
    paths = [
        ROOT / "signalmaker/jobs/ibkr_run_analysis.py",
        ROOT / "signalmaker/market_data/services.py",
        ROOT / "app/api/routes/admin_market_data.py",
    ]
    combined = "\n".join(path.read_text().lower() for path in paths)
    assert not [token for token in forbidden if token in combined]


def test_legacy_adapter_methods_delegate_to_shared_workflow():
    source = (ROOT / "signalmaker/market_data/analysis_adapter.py").read_text()
    assert "SharedMarketAnalysisService().run(" in source
    assert "WyckoffPipelineService().analyze(" not in source
    assert "MomentumService.calculate_bundle(" not in source

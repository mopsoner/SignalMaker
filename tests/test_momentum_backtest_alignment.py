from app.services.momentum_backtest_service import MomentumBacktestService


class FakeEngine:
    def __init__(self):
        self.best_entry_args = None
        self.structure_arg = None

    def best_entry_ready_asset(self, **kwargs):
        self.best_entry_args = kwargs
        return {"symbol": "ETHUSDT"}

    def structure_broken(self, asset):
        self.structure_arg = asset
        return asset.get("structure_15m_status") == "broken_bearish"


def test_backtest_entry_selection_delegates_to_live_momentum_engine():
    service = object.__new__(MomentumBacktestService)
    service.engine = FakeEngine()
    snapshot = {
        "BTCUSDT": {"symbol": "BTCUSDT", "momentum_score": 10.0},
        "ETHUSDT": {"symbol": "ETHUSDT", "momentum_score": 12.0},
    }

    result = service._best_entry(snapshot, {"min_momentum_score": 0.5}, exclude={"BTCUSDT"})

    assert result == {"symbol": "ETHUSDT"}
    assert service.engine.best_entry_args == {
        "rankings": [snapshot["ETHUSDT"], snapshot["BTCUSDT"]],
        "min_momentum_score": 0.5,
        "exclude_symbols": {"BTCUSDT"},
    }


def test_backtest_exit_check_delegates_to_live_momentum_engine():
    service = object.__new__(MomentumBacktestService)
    service.engine = FakeEngine()
    asset = {"symbol": "BTCUSDT", "structure_15m_status": "broken_bearish"}

    assert service._structure_broken(asset) is True
    assert service.engine.structure_arg == asset

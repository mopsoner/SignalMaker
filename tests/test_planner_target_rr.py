from types import SimpleNamespace

from app.services.executor_service import ExecutorService
from app.services.planner_service import PlannerService


def test_planner_low_rr_upgrade_selects_next_farther_long_target():
    planner = PlannerService()
    resolved_trade = {
        "position_side": "long",
        "entry": 100.0,
        "stop": 95.0,
        "target": 101.0,
        "target_source": "close_target",
        "target_candidates": [{"source": "close_target", "level": 101.0, "hierarchy_rank": 10}],
    }
    signal = {
        "range_high_1h": 102.0,
        "previous_day_high": 103.0,
        "range_high_4h": 105.0,
        "major_swing_high_4h": 110.0,
    }

    upgraded, rr = planner._upgrade_target_for_min_rr(resolved_trade, signal, min_rr=0.8)

    assert rr == 1.0
    assert upgraded["target"] == 105.0
    assert upgraded["target_source"] == "range_high_4h"
    assert upgraded["target_rr_upgrade"]["reason"] == "initial_target_low_rr_next_farther_structural_target"


def test_planner_low_rr_upgrade_selects_next_farther_short_target():
    planner = PlannerService()
    resolved_trade = {
        "position_side": "short",
        "entry": 100.0,
        "stop": 105.0,
        "target": 99.0,
        "target_source": "close_target",
        "target_candidates": [{"source": "close_target", "level": 99.0, "hierarchy_rank": 10}],
    }
    signal = {
        "range_low_1h": 98.0,
        "previous_day_low": 97.0,
        "range_low_4h": 96.0,
        "major_swing_low_4h": 90.0,
    }

    upgraded, rr = planner._upgrade_target_for_min_rr(resolved_trade, signal, min_rr=0.8)

    assert rr == 0.8
    assert upgraded["target"] == 96.0
    assert upgraded["target_source"] == "range_low_4h"


def test_executor_does_not_downgrade_below_candidate_rr(monkeypatch):
    monkeypatch.setattr(
        "app.services.executor_service.load_runtime_settings",
        lambda *args, **kwargs: {"strategy": {"planner_min_rr": 0.8}},
    )
    executor = object.__new__(ExecutorService)
    candidate = SimpleNamespace(
        entry_price=100.0,
        stop_price=95.0,
        target_price=110.0,
        rr_ratio=2.0,
        side="long",
        liquidity_context=None,
        execution_target={"level": 103.0, "type": "close_resistance"},
        payload={"range_high_4h": 110.0},
    )

    plan = executor._hierarchical_target_plan(candidate)

    assert plan["target_price"] == 110.0
    assert plan["position_rr"] == 2.0
    assert plan["min_reward_ratio"] == 2.0

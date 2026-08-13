import json
import os
import time

from app.core.config import settings
from app.db.session import SessionLocal
from app.services.execution.momentum_execution_service import MomentumExecutionService
from app.services.momentum_engine_service import MomentumEngineService


def run_once() -> dict:
    with SessionLocal() as db:
        decision = MomentumEngineService(db).current_decision()
        return {"decision": decision, "execution_result": MomentumExecutionService(db).execute_decision(decision)}


def assert_live_configuration() -> None:
    problems = []
    if not settings.momentum_execution_enabled:
        problems.append("MOMENTUM_EXECUTION_ENABLED must be true")
    if not settings.kraken_execution_enabled:
        problems.append("KRAKEN_EXECUTION_ENABLED must be true")
    if settings.kraken_dry_run:
        problems.append("KRAKEN_DRY_RUN must be false")
    if not settings.kraken_api_key or not settings.kraken_secret_key:
        problems.append("KRAKEN_API_KEY and KRAKEN_SECRET_KEY are required")
    if settings.momentum_execution_mode == "margin" and not settings.kraken_margin_execution_enabled:
        problems.append("KRAKEN_MARGIN_EXECUTION_ENABLED must be true for margin mode")
    if problems:
        raise RuntimeError("Unsafe/incomplete live momentum configuration: " + "; ".join(problems))


if __name__ == "__main__":
    assert_live_configuration()
    once = "--once" in __import__("sys").argv
    while True:
        print(json.dumps(run_once(), default=str))
        if once:
            break
        time.sleep(float(os.getenv("MOMENTUM_EXECUTOR_INTERVAL_SECONDS", "60")))

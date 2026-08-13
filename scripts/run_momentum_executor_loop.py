import json
import os
import time

from app.db.session import SessionLocal
from app.services.execution.momentum_execution_service import MomentumExecutionService
from app.services.momentum_engine_service import MomentumEngineService


def run_once() -> dict:
    with SessionLocal() as db:
        decision = MomentumEngineService(db).current_decision()
        return {"decision": decision, "execution_result": MomentumExecutionService(db).execute_decision(decision)}


if __name__ == "__main__":
    once = "--once" in __import__("sys").argv
    while True:
        print(json.dumps(run_once(), default=str))
        if once:
            break
        time.sleep(float(os.getenv("MOMENTUM_EXECUTOR_INTERVAL_SECONDS", "60")))

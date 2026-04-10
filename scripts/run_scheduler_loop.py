#!/usr/bin/env python3
"""
Scheduler worker — keeps the scheduler process slot alive.
Pipeline and executor are already handled by their own dedicated workers
(run_pipeline_loop.py and run_executor_loop.py) started by main.py.
This process exists so main.py can track a scheduler PID; it does no
additional work to avoid duplicating the individual workers.
"""
import time

if __name__ == "__main__":
    print("Scheduler worker started (standby — pipeline and executor run independently)", flush=True)
    while True:
        time.sleep(60)

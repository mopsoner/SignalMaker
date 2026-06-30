"""Compatibility wrapper for the Momentum decision feed.

The historical file was introduced as a sync wrapper but was left truncated in
branch history. Keep it importable and delegate to ``momentum_decision_feed`` so
legacy service/unit invocations do not fail during startup.
"""

from raspberry_executor import momentum_decision_feed as base
from raspberry_executor.logging_setup import setup_logging

logger = setup_logging("raspberry-momentum-decision-sync")


def run_once() -> dict:
    """Fetch and execute one Momentum decision using the base feed module."""
    decision = base.fetch_decision()
    result = base.execute_decision(decision)
    logger.info("momentum decision sync result=%s", result)
    return result


def main() -> None:
    print(run_once())


if __name__ == "__main__":
    main()

import logging

from raspberry_executor import main_spot_oco
from raspberry_executor.state import StateStore


class QuietSkipFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if message.startswith("skip candidate="):
            return False
        if message.startswith("candidates fetched count=") and "ids=" in message:
            return False
        return True


def patch_candidate_skipped_events() -> None:
    original_add_event = StateStore.add_event

    def add_event_without_normal_skips(self, candidate_id, event_type, payload=None, *, save=True):
        if event_type == "candidate_skipped":
            return None
        return original_add_event(self, candidate_id, event_type, payload, save=save)

    StateStore.add_event = add_event_without_normal_skips


def main() -> None:
    main_spot_oco.logger.addFilter(QuietSkipFilter())
    patch_candidate_skipped_events()
    main_spot_oco.main()


if __name__ == "__main__":
    main()

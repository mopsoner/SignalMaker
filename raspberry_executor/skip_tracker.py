class SkipTracker:
    def __init__(self) -> None:
        self.reasons: dict[str, int] = {}

    def add(self, reason: str) -> None:
        key = str(reason or "unknown")
        self.reasons[key] = self.reasons.get(key, 0) + 1

    @property
    def total(self) -> int:
        return sum(self.reasons.values())

    def summary(self) -> dict:
        return {"skipped": self.total, "skip_reasons": self.reasons}

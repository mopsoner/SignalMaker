class RiskService:
    """Compatibility stub for removed real-exchange execution paths."""

    def __init__(self, db) -> None:
        self.db = db

    def validate_live_candidate(self, **kwargs) -> None:
        raise RuntimeError('Real exchange execution is handled by the Raspberry Executor')

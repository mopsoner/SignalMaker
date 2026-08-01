class MarketUniverseService:
    """Manage market universes without embedding a provider-specific catalogue."""

    def __init__(self, repo):
        self.repo = repo

    async def seed_initial_universes_and_assets(self):
        """Retained for API compatibility; IBKR assets are discovered dynamically."""
        return {"universes": 0, "assets": 0}

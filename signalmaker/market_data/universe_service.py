from signalmaker.data_providers.eodhd.symbols import INITIAL_ASSETS, INITIAL_UNIVERSES

class MarketUniverseService:
    def __init__(self, repo):
        self.repo = repo

    async def seed_initial_universes_and_assets(self):
        universe_ids = {}
        for name, description, region, asset_type, currency in INITIAL_UNIVERSES:
            universe_ids[name] = await self.repo.create_or_update_universe(name, description, region, asset_type, currency)
        count = 0
        for universe, symbol, name, asset_type, pea, ucits in INITIAL_ASSETS:
            exchange = symbol.split('.')[-1]
            await self.repo.upsert_market_asset(
                universe_ids.get(universe), symbol, symbol, exchange, name, asset_type,
                "France" if exchange == "PA" else "US", "France" if exchange == "PA" else "United States",
                "EUR" if exchange == "PA" else "USD", pea_eligible=pea, ucits=ucits, enabled=True,
            )
            count += 1
        return {"universes": len(universe_ids), "assets": count}

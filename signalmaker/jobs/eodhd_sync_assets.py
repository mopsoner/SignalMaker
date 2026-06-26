import asyncio
from app.db.session import SessionLocal
from signalmaker.data_providers.eodhd.repository import EODHDRepository
from signalmaker.market_data.universe_service import MarketUniverseService

async def main():
    with SessionLocal() as db:
        repo = EODHDRepository(db); repo.ensure_schema()
        result = await MarketUniverseService(repo).seed_initial_universes_and_assets()
        db.commit(); print(f"OK synced assets: {result}")

if __name__ == "__main__":
    asyncio.run(main())

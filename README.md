# SignalMaker

Phase 1 of the Replit VM architecture refactor.

## What is included
- FastAPI application layer
- Centralized settings with `.env`
- SQLAlchemy 2 setup
- PostgreSQL-ready database configuration
- `asset_state_current` model as the first canonical live state table
- Health endpoints and asset-state API endpoints
- Boot script for API and DB initialization

## Project structure
```text
app/
  api/
    routes/
  core/
  db/
  models/
  schemas/
  services/
scripts/
```

## Quick start
```bash
cp .env.example .env
bash run.sh init-db
bash run.sh api
```

## Main endpoints
- `GET /healthz`
- `GET /api/v1/health`
- `GET /api/v1/assets`
- `GET /api/v1/assets/{symbol}`
- `POST /api/v1/assets/{symbol}`

## Notes
- The default `.env.example` uses PostgreSQL.
- For local quick tests you can temporarily set:
  - `DATABASE_URL=sqlite:///./signalmaker.db`
- This is only Phase 1. Collector, signal engine, planner, executor, scheduler and UI separation come next.

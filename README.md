# SignalMaker

Phase 1 and early Phase 2 of the Replit VM architecture refactor.

## What is included
- FastAPI application layer
- Centralized settings with `.env`
- SQLAlchemy 2 setup
- PostgreSQL-ready database configuration
- Canonical live tables:
  - `asset_state_current`
  - `live_runs`
  - `trade_candidates`
  - `positions`
- Health endpoints and live API endpoints
- Initial service separation:
  - collector service with Binance public REST support
  - signal engine wired to the legacy v231 Wyckoff logic
  - planner service generating trade candidates
- Pipeline endpoint for an end-to-end run-once cycle
- Boot script for API and DB initialization

## Main endpoints
- `GET /healthz`
- `GET /api/v1/health`
- `GET /api/v1/services`
- `GET /api/v1/assets`
- `GET /api/v1/assets/{symbol}`
- `POST /api/v1/assets/{symbol}`
- `GET /api/v1/live-runs`
- `GET /api/v1/trade-candidates`
- `GET /api/v1/positions`
- `POST /api/v1/pipeline/run-once?limit=5`

## Quick start
```bash
cp .env.example .env
bash run.sh init-db
bash run.sh api
```

Then trigger one cycle:
```bash
curl -X POST "http://localhost:8080/api/v1/pipeline/run-once?limit=5"
```

## Notes
- PostgreSQL remains the target runtime on Replit VM.
- SQLite still works for local smoke tests.
- Next step: add dedicated workers for collector, engine and planner, then wire an executor.

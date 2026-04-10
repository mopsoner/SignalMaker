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
  - collector service stub
  - signal engine service stub
  - planner service stub
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
- `GET /api/v1/services`
- `GET /api/v1/assets`
- `GET /api/v1/assets/{symbol}`
- `POST /api/v1/assets/{symbol}`
- `GET /api/v1/live-runs`
- `GET /api/v1/trade-candidates`
- `GET /api/v1/positions`

## Notes
- PostgreSQL remains the target runtime on Replit VM.
- SQLite still works for local smoke tests.
- Next step: migrate the existing strategy logic into the `signal_engine` service and start writing real `trade_candidates` rows.

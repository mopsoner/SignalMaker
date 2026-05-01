# SignalMaker

FastAPI trading app (Binance public REST, paper trading) with React Vite dashboard.

## Architecture

- **Backend**: FastAPI + SQLAlchemy (psycopg v3) → Replit PostgreSQL (local dev) / Neon PostgreSQL (production)
- **Frontend**: React + Vite at `frontend/`, served by FastAPI static files in production
- **Deployment**: VM target — `python main.py` starts uvicorn on port 5000 → external 80

## Code Locations

| Directory | Purpose |
|---|---|
| `SignalMaker/app/` | Local dev code (used by `SignalMaker API` workflow) |
| `app/` | Production code (used by `python main.py` deployment) |
| `frontend/` | React Vite dashboard (shared) |
| `scripts/` | Pipeline loop scripts |
| `main.py` | Production entrypoint — sets env vars, starts uvicorn |

## Workflows

- **SignalMaker API** (`bash SignalMaker/run.sh api`): local dev API on port 8080
- **Start application** (`npm run dev --prefix frontend`): React dev server

## Key Configuration

- `DATABASE_URL`: Auto-converted `postgres://` → `postgresql+psycopg://` via `@field_validator` in `app/core/config.py`
- **Binance**: `api.binance.us` (NOT `api.binance.com` — datacenter IPs are 451-blocked)
- Settings persisted in `AppSetting` DB table; defaults come from `config.py`

## Data Model Notes

- `market_candles.open_time` / `close_time`: **BIGINT** (millisecond Unix timestamps exceed INTEGER max)

## API Endpoints

- `GET /api/v1/health` — health check
- `GET /api/v1/services` — status of collector, signal engine, planner, scheduler
- `POST /api/v1/pipeline/run-once?limit=N` — run full pipeline for N symbols
- `GET /api/v1/live-runs` — history of pipeline runs
- `GET /api/v1/trade-candidates` — generated trade signals
- `GET /api/v1/market-data/candles` — stored candle data

## Pipeline Flow

1. **Collector** fetches top-N symbols from Binance + 1m candles (180 bars)
2. **Signal Engine** applies `legacy_wyckoff_v231` strategy
3. **Planner** filters by min_score ≥ 4.0 and min_rr ≥ 0.8
4. Results stored in PostgreSQL; live-runs logged with stats

## Production Deployment

```
deploymentTarget = "vm"
run = ["python", "main.py"]
build = ["bash", "-c", "pip install -r requirements.txt && cd frontend && npm install && npm run build"]
```
Port 5000 → external 80.

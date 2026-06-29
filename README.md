# SignalMaker

Phases 1 to 4 are now scaffolded in a runnable form for Replit VM.

## Included
- FastAPI application layer
- Centralized settings with `.env`
- SQLAlchemy 2 setup
- PostgreSQL-ready database configuration
- Lightweight HTML/CSS/JS frontend dashboard (no React, Vite, npm, or esbuild required)
- Live tables:
  - `asset_state_current`
  - `live_runs`
  - `trade_candidates`
  - `positions`
  - `orders`
  - `fills`
  - `market_candles`
- Service separation:
  - collector service using Binance public REST
  - signal engine wired to legacy v231 logic
  - planner service generating trade candidates
  - executor service for paper trading
  - scheduler service plus simple worker loops
- Pipeline and executor API endpoints
- VM deployment helper script
- Production env sample and systemd templates

## Frontend
The frontend in `frontend/` is intentionally simple: separate static HTML pages, CSS and JavaScript.
Those pages are copied directly to `frontend/dist` and do not use Node, npm, Vite or esbuild, so it stays compatible with older Raspberry Pi / ARMv6 devices.

The lightweight frontend keeps the pages that existed before the Kraken work: status, momentum candidate sync, positions, momentum settings, Wyckoff/SMC dashboard, trade candidates, ops, logs, market data admin, and asset detail.

Build or refresh the static files with:
```bash
bash scripts/build_frontend.sh
```

Start the frontend with:
```bash
bash scripts/start_frontend.sh
```

The backend also serves `frontend/dist` when it exists, so `http://IP_DU_RASPBERRY:5000` can show the same lightweight UI next to the API.

## Main endpoints
- `GET /healthz`
- `GET /api/v1/health`
- `GET /api/v1/services`
- `GET /api/v1/assets`
- `GET /api/v1/live-runs`
- `GET /api/v1/trade-candidates`
- `GET /api/v1/positions`
- `GET /api/v1/orders`
- `GET /api/v1/fills`
- `GET /api/v1/market-data/candles`
- `POST /api/v1/pipeline/run-once?limit=5`
- `POST /api/v1/executor/run-once?limit=10&quantity=1`

## Quick start
```bash
cp .env.example .env
bash run.sh init-db
bash run.sh all
```

`bash run.sh all` launches the backend first, waits for `http://127.0.0.1:${APP_PORT:-5000}/healthz`, then starts the frontend. You can tune the wait with `API_STARTUP_TIMEOUT` (default: 60 seconds).

## Raspberry Pi install
Run the full Raspberry Pi setup from a fresh checkout with one command:

```bash
cd ~/Desktop
git clone -b "raspberry/executor-app" https://github.com/mopsoner/SignalMaker.git SignalMaker
cd SignalMaker
bash scripts/install_raspberry.sh
```

The installer provisions PostgreSQL locally, creates the `signalmaker` database, installs Raspberry-specific Python dependencies, builds the lightweight static frontend by copying HTML/CSS/JS into `frontend/dist`, initializes the schema, and enables the SignalMaker systemd services. It does not run Vite, npm or esbuild on the Raspberry Pi.

### Lightweight frontend for older Raspberry Pi devices
Older Raspberry Pi devices / `armv6l` can fail on Vite/esbuild with `Bus error`. SignalMaker now avoids that path: run `bash scripts/build_frontend.sh` to refresh `frontend/dist` with static files only.

### Raspberry UI
After installation, start the backend API and frontend UI services:

```bash
sudo systemctl start signalmaker-api signalmaker-frontend
```

Open SignalMaker from another device on the same network:

- API: `http://IP_DU_RASPBERRY:5000`
- UI: `http://IP_DU_RASPBERRY:3000`

Useful Raspberry debug commands:

```bash
uname -m
systemctl status signalmaker-api
systemctl status signalmaker-frontend
journalctl -u signalmaker-frontend -f
curl http://localhost:5000/healthz
curl -I http://localhost:3000/index.html
curl -I http://localhost:3000/dashboard.html
```

### Raspberry frontend/API port debugging

On Raspberry Pi installs, port `3000` is only the static frontend served from `frontend/dist`, while port `5000` is the backend API. The frontend should load pages such as `index.html` and `dashboard.html` from `http://IP_DU_RASPBERRY:3000`, but API calls must target `http://IP_DU_RASPBERRY:5000`.

Use these checks when debugging frontend/API routing:

```bash
curl http://localhost:5000/healthz
curl -I http://localhost:3000/index.html
curl -I http://localhost:3000/dashboard.html
```

Requests under `/api/v1/...` must never be served by the static frontend on port `3000`; they should go to the backend API on port `5000`. If the frontend server logs show 404s for `/api/v1/...`, rebuild `frontend/dist` with `bash scripts/build_frontend.sh` and restart the frontend service with `sudo systemctl restart signalmaker-frontend`.

### Raspberry CORS preflight debugging

If browser API calls from `http://IP_DU_RASPBERRY:3000` fail with `OPTIONS ... 400 Bad Request`, verify the backend CORS preflight response from the Raspberry Pi:

```bash
curl -i -X OPTIONS "http://localhost:5000/api/v1/health" \
  -H "Origin: http://RASPBERRY_IP:3000" \
  -H "Access-Control-Request-Method: GET"
```

The response should include:

```text
HTTP/1.1 200 OK
access-control-allow-origin: http://RASPBERRY_IP:3000
```

If the allowed origin is missing, check that `.env` contains either the Raspberry IP in `CORS_ORIGINS` or a LAN-compatible `CORS_ORIGIN_REGEX`, then restart the API service:

```bash
grep -E '^CORS_ORIGINS=|^CORS_ORIGIN_REGEX=' .env
sudo systemctl restart signalmaker-api
```

To keep the Raspberry Pi running without the UI temporarily, disable only the frontend service:

```bash
sudo systemctl stop signalmaker-frontend
sudo systemctl disable signalmaker-frontend
```

## VM deploy helper
```bash
bash scripts/deploy_vm.sh
bash scripts/bootstrap_all.sh
```

## Start processes
```bash
bash scripts/start_api.sh
bash scripts/start_pipeline_worker.sh
bash scripts/start_executor_worker.sh
bash scripts/start_scheduler_worker.sh
bash scripts/start_frontend.sh
```

## Production env
```bash
cp .env.production.example .env
```
Then edit the database URL and runtime values.

## systemd templates
Templates are available in `deploy/systemd/`.

## Notes
- This is now a functional scaffold, not a finished production trading system.
- It still needs hardening for real live trading: risk engine, exchange auth, order reconciliation, stop/TP sync, worker supervision, retries, and UI migration.

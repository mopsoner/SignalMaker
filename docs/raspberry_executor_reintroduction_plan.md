# Raspberry executor business logic reintroduction report

## Scope completed

This branch keeps the clean revert of PR #154 (`ce9738a3ffcb7a116eea024e644f87ead0f3de25`) and reintroduces the Raspberry executor business flow in the current FastAPI/static-frontend architecture. The implementation keeps SignalMaker remote polling, local autonomous execution, Momentum candidate sync/execution visibility, and Kraken/Kraken exchange selection while removing active IBKR/stocks/ETF code and pages.

## Historical reference used

- First IBKR commit supplied by the request: `378421511595fc4b30cf78f1bc0e55e1954f1353` (`Add isolated IBKR market data provider`).
- Pre-IBKR reference: `6188f6d75392d979ab3776df334bc0e4399e5890` (`Merge pull request #76 from mopsoner/codex/fix-double-counting-of-pnl-in-calculations`).
- Production-oriented Raspberry executor references before IBKR/Kraken refactors include the Momentum executor sequence ending around `dc9c7d9` / `b341abb` and fixes such as `540dab6`, `034ce3c`, `c7ee7c2`, `5815fde`, `27bd45d`; these were used as read-only business references, not applied wholesale.

## Trade-candidate logic restored/adapted

- `raspberry_executor/signalmaker_client.py` remains the remote SignalMaker client for `/api/v1/trade-candidates`, Momentum ranking/sync endpoints, settings fetch, cursor filtering, and local candidate import.
- `raspberry_executor/run_once.py` provides the requested one-shot executor command for `SIGNALMAKER_BASE_URL=<remote> python -m raspberry_executor.run_once`.
- `raspberry_executor/main.py` now uses the exchange factory so the same candidate flow can execute on Kraken or Kraken.
- `app/services/executor_service.py` executes local open candidates through an exchange adapter, creates entry/TP/optional stop local orders, persists positions/fills, and marks candidates executed.
- `app/api/routes/trade_candidates.py` and `app/api/routes/executor.py` expose the local candidate list, executed marker, executor run-once, reconcile, and Momentum candidate sync endpoints.

## Momentum buy/sell logic restored/adapted

- `raspberry_executor/momentum_decision_feed.py` remains the Momentum buy/sell/rotation business engine and defaults back to spot mode unless margin is explicitly configured.
- `raspberry_executor/momentum_decision_feed_sync.py` is importable and delegates to the Momentum decision feed.
- Momentum ranking/payload sync into local trade candidates has been retired; runtime execution now consumes already-open candidates only.
- The frontend/TUI-visible data now shows Momentum candidates from local trade candidates with entry, TP, stop, status, score, and RSI-derived payload fields.

## Historical endpoints available

- `GET /api/v1/admin/settings`
- `GET /api/v1/trade-candidates?status=open&limit=...`
- `POST /api/v1/trade-candidates/{candidate_id}/executed`
- `POST /api/v1/executor/run-once`
- `POST /api/v1/executor/reconcile`
- `GET /api/v1/momentum`
- `GET /api/v1/momentum/ranking`
- `GET /api/v1/positions`
- `GET /api/v1/orders`
- `GET /api/v1/fills`

## Pages restored/adapted

- `index.html`: executor status.
- `candidates.html`: trade candidates with side, entry, TP, stop, score, status.
- `momentum-candidates.html`: Momentum candidate sync plus buy/sell tracking fields.
- `positions.html`: open/closed positions with entry, mark, TP, PnL.
- `orders.html`: entry, TP, stop orders and fills.
- `ops.html`: official Settings / Logs / Admin page for runtime settings, SignalMaker URL, Kraken credentials, live/paper controls, Momentum settings, workers, and executor logs.

## New architecture kept

- Single-origin FastAPI application serving API plus static frontend.
- Port `5000` runtime.
- Runtime/admin settings model.
- TUI/kiosk launchers aligned to current API/page names.
- Kraken credentials and isolated Kraken adapter.
- Clean local state/cursor stores for autonomous Raspberry execution.

## Explicitly excluded

The active code paths and static pages no longer contain `IBKR`, `ibkr`, `stocks-etfs`, `stocks_etfs`, `ETF`, or `STOCK` matches under `app`, `frontend`, `raspberry_executor`, `scripts`, or `README.md`.

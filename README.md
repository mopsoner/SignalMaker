# SignalMaker

## IBKR Europe stock/ETF model

IBKR market data uses two primary universes: **Europe Stocks** and **Europe ETF**.
PEA eligibility and UCITS status are asset attributes rather than universes. Assets
also carry `provider`, `provider_symbol`, `asset_type`, `region`, `country`,
`currency`, `exchange_code`, `pea_eligible`, `ucits`, and free-form `metadata`.

Useful filter views are:

* **PEA Eligible:** `region=EU&pea_eligible=true`
* **ETF PEA:** `asset_type=ETF&region=EU&pea_eligible=true&ucits=true`
* **France Stocks:** `asset_type=STOCK&region=EU&country=FR`
* **Amsterdam**, **Xetra**, **London**, and **Switzerland:** select assets with
  exchange codes `AMS`, `XETRA`, `LSE`, and `SWX`, respectively.

The candle ingest endpoint continues to accept `Stocks Euronext Paris`, `Stocks
Europe`, `ETF PEA`, and `ETF Europe UCITS` from older executor-app releases, but
new integrations should send one of the two primary universe names. Legacy
`IBKR Imported` records are retained only when the payload lacks enough asset type
and region information to select a primary universe.

Phases 1 to 4 are now scaffolded in a runnable form for Replit VM.

## Included
- FastAPI application layer
- Centralized settings with `.env`
- SQLAlchemy 2 setup
- PostgreSQL-ready database configuration
- React + Vite frontend dashboard
- Live tables:
  - `asset_state_current`
  - `live_runs`
  - `trade_candidates`
  - `positions`
  - `orders`
  - `fills`
  - `market_candles`
- Service separation:
  - market-data ingestion API fed by the Raspberry Executor
  - signal engine wired to legacy v231 logic
  - planner service generating trade candidates
  - `wyckoff_paper` worker for simulated Wyckoff/SMC trading
  - scheduler service plus simple worker loops
- Pipeline and executor API endpoints
- VM deployment helper script
- Production env sample and systemd templates

## Frontend
The React/Vite app lives in `frontend/`.

Start it with:
```bash
bash scripts/start_frontend.sh
```

Leave `VITE_API_BASE` unset for the normal same-origin deployment. The frontend
server proxies `/api`, `/admin`, and `/healthz` to the backend. Set an alternate
base only when the browser can reach that backend URL (for example, during
development with the API on another host):
```bash
VITE_API_BASE=https://api.example.com npm run build
```

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
bash run.sh api
bash scripts/start_frontend.sh
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
bash scripts/start_wyckoff_paper_worker.sh
bash scripts/start_scheduler_worker.sh
bash scripts/start_frontend.sh
```

## Production env
```bash
cp .env.production.example .env
```
Then edit the database URL and runtime values.

### Préflight Kraken avant la production

Avant d'activer l'exécution réelle, lancez le préflight avec les mêmes clés API,
symboles et modes que la production :

```bash
# Le mode par défaut est MOMENTUM_LIVE_MODE.
bash run.sh kraken-preflight --symbol BTCUSD --quote-amount 50

# Vérifier explicitement les payloads spot et margin, achat et vente.
bash run.sh kraken-preflight --symbol BTCUSD --quote-amount 50 \
  --mode spot --mode margin
```

Le préflight appelle réellement `AssetPairs`, `Ticker`, `OHLC`, `Balance`,
`OpenOrders` et `OpenPositions`. Il envoie aussi chaque variante de `AddOrder`
avec `validate=true` : Kraken valide donc la signature, les permissions, la
paire, le volume et le levier **sans créer d'ordre**. La commande ne journalise
ni les clés ni les soldes et renvoie un code non nul dès qu'au moins un contrôle
échoue. `QueryOrders` et `CancelOrder` ne sont pas lancés : ils nécessitent un
identifiant d'ordre réel et ne disposent pas d'un mode de validation sans effet.

Ne passez `KRAKEN_DRY_RUN=false` et n'activez les workers d'exécution qu'après un
résultat global `"ok": true`. Les clés doivent autoriser la consultation des
fonds/ordres et la création d'ordres ; le préflight ne requiert aucun droit de
retrait.

### Démarrer le momentum live

Le moteur momentum est long-only ; commencez en spot. Après un préflight réussi,
configurez les valeurs suivantes dans le `.env` de production :

```dotenv
KRAKEN_EXECUTION_ENABLED=true
KRAKEN_DRY_RUN=false
MOMENTUM_LIVE_ENABLED=true
MOMENTUM_LIVE_MODE=spot
MOMENTUM_LIVE_INTERVAL_SECONDS=60
```

Puis installez et démarrez le worker dédié :

```bash
sudo cp deploy/systemd/signalmaker-momentum-live.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now signalmaker-momentum-live
sudo systemctl status signalmaker-momentum-live
```

Le worker refuse de démarrer si les clés, les garde-fous Kraken ou le mode live
ne sont pas cohérents. Chaque `decision_id` live est journalisé avant le prochain
cycle afin qu'un redémarrage ou une décision persistée ne soumette pas deux fois
le même ordre. Pour arrêter immédiatement les nouvelles soumissions :

```bash
sudo systemctl stop signalmaker-momentum-live
```

### Démarrer Wyckoff / SMC en live

Le worker `wyckoff_paper` reste dédié au portefeuille paper. L'exécution
réelle des candidats Wyckoff/SMC utilise un worker séparé afin qu'elle puisse
être arrêtée sans interrompre le moteur d'analyse ni le paper trading.

Après un préflight Kraken réussi, configurez :

```dotenv
KRAKEN_EXECUTION_ENABLED=true
KRAKEN_DRY_RUN=false
WYCKOFF_LIVE_ENABLED=true
WYCKOFF_LIVE_MODE=spot
WYCKOFF_LIVE_INTERVAL_SECONDS=60
WYCKOFF_LIVE_LIMIT=10
WYCKOFF_LIVE_QUANTITY=1
```

Puis démarrez le service dédié :

```bash
sudo cp deploy/systemd/signalmaker-wyckoff-live.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now signalmaker-wyckoff-live
```

Le spot accepte seulement les candidats haussiers. Pour ouvrir des positions
baissières, utilisez `WYCKOFF_LIVE_MODE=margin` et activez explicitement
`KRAKEN_MARGIN_EXECUTION_ENABLED` ainsi que `KRAKEN_MARGIN_SHORTS_ENABLED`.

## systemd templates
Templates are available in `deploy/systemd/`.

### Momentum cadence setting

The momentum worker cadence is stored in `AppSetting` with category `momentum`
and key `momentum_paper_cadence_hours`. Its default is one hour. Select 1, 4, 8,
or 24 hours on the Momentum page or in **Admin Settings → Bot runtime → Momentum
engine cadence**. The selection is persisted immediately and is used by the
momentum worker on its next tick.

Crypto momentum purchases (including rotations) require a momentum score of at
least 20 points, even if runtime settings specify a lower minimum. A higher
configured minimum is respected.

Crypto momentum entries are blocked when RSI(14) is strictly above 80 on 15m
or 1h, or for one hour after either RSI crosses from >= 80 to < 80. Cross times
use candle closure times; restarting the worker does not reset the window.
A negative return between the two most recent closed 15m candles triggers a
sale on the next worker tick without waiting for the hourly cadence. Unclosed
candles are excluded. Existing rotation behavior applies: an eligible replacement
may be bought after the sale, otherwise the engine stays in cash. Assets with a
negative closed 15m return cannot be bought while that exit signal is active.
These filters do not affect stock/ETF momentum.

## Notes
- This is now a functional scaffold, not a finished production trading system.
- It still needs hardening for real live trading: risk engine, exchange auth, order reconciliation, stop/TP sync, worker supervision, retries, and UI migration.

### Goal global Kraken

`WYCKOFF_LIVE_GOAL_USD=100` définit le seuil global (strictement positif).
Le worker dédié `kraken_profit`, activé par `KRAKEN_PROFIT_WORKER_ENABLED=true`,
scanne Kraken sans dépendre de l'ouverture de la page Positions. La carte Goal
somme les profits latents des positions sur marge de tout le compte et des
positions spot live suivies localement. Les positions sur marge externes sont
importées dans Positions. Les profits en devises autres que USD sont convertis
au cours Kraken ; une conversion indisponible bloque le déclenchement.
Les soldes spot externes ne constituent pas des positions avec prix d'entrée et
ne sont pas vendus automatiquement. Le profit est brut, avant frais et slippage.

À partir du seuil, les ordres de sortie sont annulés puis des ordres au marché
sont soumis (reduce-only sur marge). La clôture est confirmée après exécution,
et les nouvelles entrées du worker sont suspendues pendant la liquidation.
`KRAKEN_EXECUTION_ENABLED=true`, `KRAKEN_DRY_RUN=false` et
`KRAKEN_PROFIT_WORKER_ENABLED=true` sont requis. Les accès Kraken du serveur et un
worker profit actif sont nécessaires ; le Raspberry Executor externe doit coordonner
ses nouvelles entrées avec cette liquidation. Le GET `/api/v1/positions/goal`
est en lecture seule ; `/api/v1/executor/reconcile` exige la clé opérateur.
En cas de réponse perdue lors d'une soumission, la position reste `closing`
avec une erreur pour réconciliation manuelle, afin de ne pas répéter une vente.

### Worker profit indépendant

Le worker Wyckoff live et le worker profit démarrent automatiquement avec l’API
par défaut, sous réserve de configuration Kraken valide :

```dotenv
WYCKOFF_LIVE_ENABLED=true
KRAKEN_PROFIT_WORKER_ENABLED=true
KRAKEN_PROFIT_WORKER_INTERVAL_SECONDS=15
WYCKOFF_LIVE_GOAL_USD=100
```

Pour désactiver un worker, passez son flag à `false` puis redémarrez l’API.
Une valeur explicite dans le `.env` prime sur la valeur par défaut. Le lancement
manuel reste possible avec `bash run.sh kraken-profit-loop` (ou
`kraken-profit-once` pour un seul passage).
Le worker dispose d'une session DB par passage, d'un heartbeat et de logs dédiés.
Il continue après une erreur Kraken. Il est également disponible dans Ops/Logs
sous `kraken_profit` et via `deploy/systemd/signalmaker-kraken-profit.service`.
Le scheduler et l'executor Wyckoff ne scannent plus Kraken pour le Goal ;
l'executor conserve un contrôle local des liquidations avant toute nouvelle entrée.
Le profit worker peut surveiller le compte même si `WYCKOFF_LIVE_ENABLED=false`.

Après une modification du `.env`, redémarrez le worker et l'API pour recharger
la configuration. Avec le flag désactivé, le worker sort sans interroger Kraken
et l'endpoint de réconciliation ne déclenche aucune clôture. Arrêter le worker
ne révoque pas les ordres déjà envoyés à Kraken.

### Démarrage commun des workers

Tous les modes de lancement de l’API utilisent le même gestionnaire. Les workers
vitaux démarrent automatiquement : Pipeline, Wyckoff paper, Momentum paper,
Scheduler, Kraken Candle Feed, IBKR ingestion et Stock/ETF analysis.
Leurs réglages métier existants restent applicables : un processus actif peut
être en pause ou attendre une tâche lorsque sa fonction est désactivée.

Les trois workers d’exécution réelle suivent exclusivement leurs flags `.env` :

```dotenv
WYCKOFF_LIVE_ENABLED=true
KRAKEN_PROFIT_WORKER_ENABLED=true
MOMENTUM_LIVE_ENABLED=false
```

Wyckoff live et Profit sont activés par défaut ; Momentum live reste désactivé
par défaut. Les valeurs explicites du `.env` priment. Après modification,
redémarrez l’API. Les protections des clés, du mode réel Kraken et de la marge
restent obligatoires. Une erreur de lancement n’empêche pas les autres workers
ni l’API de démarrer ; les logs identifient le worker concerné.
Les scripts VM ne lancent plus de workers directement, ce qui évite les doubles
lancements. Les contrôles Ops/Logs et le superviseur systemd restent utilisables.

### Global momentum AI review

Momentum → IA (`/momentum-ai`) includes an on-demand global review of the crypto momentum
portfolio, recent trades and decisions, ranking indicators and stored Kraken
15m/1h/4h candles. It proposes evidence-based improvements to test; it does not
change trading rules or execute orders. Reports remain in the current browser
session and are not persisted.

Set server-side `OPENAI_API_KEY` and optionally `OPENAI_MODEL` (default
`gpt-4.1-mini`), allow HTTPS to `api.openai.com`, and restart the API.
Use the configured operator key in the UI; `POST /api/v1/momentum-engine/analysis`
requires operator authentication. Each click sends a bounded snapshot to OpenAI
and incurs API usage. Missing credentials return 503, no data returns 422 and
provider failures return 502 without exposing upstream credentials or payloads.
The report states its timestamp, sampling limits and data limitations. Orders
without a link to sampled momentum positions are excluded. Validate proposed
rules out of sample with fees and slippage before applying them.

The IA page offers global, loss-audit, entry/exit and improvement-plan modes,
an optional question and 1/7/30-day trade and decision history. Current positions
and ranking remain current; candle context remains capped at 32 bars per interval.
A local preview (`GET /api/v1/momentum-engine/analysis/preview?days=7`, operator
key required) shows sample counts, loss counts, missing PnL and candle coverage
without calling OpenAI. Up to ten reports can be consulted while the page stays
open and exported to Markdown. Reports are not persisted across page reloads.

The model can also invoke `read_momentum_data` during analysis. The server
validates structured arguments and dispatches to existing `MomentumService`
ranking and `MomentumEngineService.current_decision` read functions, or scoped
SQLAlchemy SELECT queries over existing positions, trades, orders, decisions and
Kraken candle models. Historical candle requests accept an ISO `before` timestamp;
row queries support pagination (200 rows/call, offset at most 5000). Order reads
are linked to crypto momentum positions. No arbitrary SQL, Python evaluation,
order execution, ingestion, configuration writes or recalculation functions are
exposed. These tools query stored data, not fresh exchange prices.

A report uses at most eight tool calls over four tool rounds and a final synthesis,
with a 150-second budget checked before each provider request. Each call's arguments
and success/error status appear in the UI and Markdown export. Database read
transactions are released between provider requests. OpenAI storage is disabled
for these requests (`store: false`). Tool integration is covered by mocked API
round trips and database tests; a live OpenAI run still requires credentials and
network access.

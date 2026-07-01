# Legacy Raspberry `.env` keys

`raspberry_executor/env_store.py` now rewrites `.env` with only the bootstrap keys still read by the current Raspberry executor, momentum decision feed, candle feed, and startup scripts.

The following historical keys are intentionally treated as legacy and are no longer emitted by `write_env()`:

| Key | Status / migration note |
| --- | --- |
| `ADMIN_PASSWORD` | Deprecated local dashboard secret; not read by the current FastAPI startup path. |
| `ALLOWED_SYMBOLS` | Migrated to `QUOTE_ASSETS` when `QUOTE_ASSETS` is absent. |
| `ALLOW_SHORTS` | Replaced by runtime margin/short settings. |
| `CANDLE_FEED_QUOTE_ASSETS` | Migrated to `QUOTE_ASSETS` when `QUOTE_ASSETS` is absent. |
| `CANDLE_FEED_QUOTES` | Migrated to `QUOTE_ASSETS` when `QUOTE_ASSETS` is absent. |
| `CANDLE_FEED_SYMBOLS` | One-off override; pass it in the process environment instead of persisting it in `.env`. |
| `EXECUTION_QUOTE_ASSET` | Migrated to `QUOTE_ASSETS` when `QUOTE_ASSETS` is absent. |
| `EXECUTOR_DASHBOARD_PORT` | Deprecated split-dashboard port; the UI is served by the API on `EXECUTOR_API_PORT` / `APP_PORT`. |
| `MOMENTUM_DECISION_FALLBACK_LIMIT` | Unused by the current momentum decision feed. |
| `MOMENTUM_DECISION_STARTING_CAPITAL` | Unused by the current momentum decision feed. |

Migration behavior: when `ensure_env()` sees an existing `.env`, it parses the old file, keeps values for supported keys, maps quote-related legacy aliases into `QUOTE_ASSETS` if needed, then rewrites a minimal `.env` without legacy variables.

# Raspberry executor app

The Raspberry executor app reads its backend API origin from the runtime environment variable `API_BASE`.
The value is not hard-coded in Python, the launch script, or the systemd template; copy `.env.example` to `.env` and set the variable there for local/systemd runs.

## Required API configuration

```bash
API_BASE=https://<your-signalmaker-api-domain>
MOMENTUM_CANDIDATES_SYNC_ENABLED=true
```

At startup the app validates and logs `API_BASE`, refuses empty URLs, and refuses the known typo `mysginalmaker`.
It checks both `/api/v1/health` and `/api/v1/momentum-candidates?limit=1`; if either returns HTML instead of JSON, the app logs `remote_api_returned_html` with `status_code`, `content_type`, `url`, and `body_preview`.

## Endpoint check before deploying

Verify that `API_BASE` points to the backend API, not the frontend web app:

```bash
curl -i "$API_BASE/api/v1/momentum-candidates?limit=1"
```

The response must be `application/json`, not `text/html`.

## Momentum candidates sync

The legacy decision-feed URL is no longer used.
The executor syncs candidates from `/api/v1/momentum-candidates`.

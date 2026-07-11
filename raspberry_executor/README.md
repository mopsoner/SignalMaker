# SignalMaker Raspberry Executor

This repository contains the lightweight Raspberry executor used from the `main` branch.

The Raspberry does not analyse markets. SignalMaker creates the trade candidates. The Raspberry only reads open candidates, places orders, reports the execution, then sends final execution events when TP, SL, cancellation, close, or error happens.

## Install

Clone this repository on the Raspberry using the `main` branch.

Then create a Python virtual environment, install `requirements-raspberry.txt`, copy `.env.example` to `.env`, and edit the values. The current template only contains the supported startup variables: API/database settings, Kraken credentials, quote assets, dry-run/live controls, and minimal polling/feed/decision settings.

Keep `DRY_RUN=true` for the first tests.

## Run manually

Use the single official starter:

`./run.sh device`

Startup chain:

`@reboot crontab -> bash run.sh -> ./run.sh device -> scripts/start_api.sh -> wait for /healthz -> python -m raspberry_executor.run_all_v2`

`run_all_v2` remains the internal engine. Do not launch it directly for normal Raspberry operation. `scripts/start_raspberry_executor.sh` is kept only as a deprecated compatibility wrapper that delegates to `./run.sh device`.

## Startup

The Raspberry installer registers a single user crontab entry so `bash run.sh` starts at boot. To inspect it, run `crontab -l`. Logs are appended to `logs/startup.log`.

## Safety

The app keeps a local SQLite state database to avoid duplicate execution after restart. The Raspberry reads `/api/v1/momentum` diagnostic rankings from `main` and creates BUY/HOLD/ROTATE decisions locally, unless `MOMENTUM_DECISION_PATH` is explicitly set to a custom decision endpoint.

## UI contract source of truth

`raspberry_executor/ui_contract.py` is the source of truth for normalized local
terminal and web views. When adding or changing local navigation screens, define
or update the contract view there first so Terminal UIs and FastAPI-backed web
pages share the same `title`, `labels`, `keys`, `rows`, `summary`,
`empty_message`, and `errors` field names.

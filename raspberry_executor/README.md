# SignalMaker Raspberry Executor

This repository contains the lightweight Raspberry executor used from the `main` branch.

The Raspberry does not analyse markets. SignalMaker creates the trade candidates. The Raspberry only reads open candidates, places orders, reports the execution, then sends final execution events when TP, SL, cancellation, close, or error happens.

## Install

Clone this repository on the Raspberry using the `main` branch.

Then create a Python virtual environment, install `requirements-raspberry.txt`, copy `.env.raspberry.example` to `.env`, and edit the values.

Keep `DRY_RUN=true` for the first tests.

## Run manually

Run the module:

`python -m raspberry_executor.run_all_v2`

## systemd

A template is available at `systemd/raspberry-executor.service`.

Install it into `/etc/systemd/system/`, reload systemd, enable the service, then start it.

## Safety

The app keeps a local SQLite state database to avoid duplicate execution after restart. The Raspberry reads `/api/v1/momentum` diagnostic rankings from `main` and creates BUY/HOLD/ROTATE decisions locally, unless `MOMENTUM_DECISION_PATH` is explicitly set to a custom decision endpoint.

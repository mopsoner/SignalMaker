# SignalMaker Raspberry Executor

This branch contains only the lightweight Raspberry executor.

The Raspberry does not analyse markets. SignalMaker creates the trade candidates. The Raspberry only reads open candidates, places orders, reports the execution, then sends final execution events when TP, SL, cancellation, close, or error happens.

## Install

Clone this repository on the Raspberry using the `raspberry/executor-app` branch.

Then create a Python virtual environment, install `requirements-raspberry.txt`, copy `.env.raspberry.example` to `.env`, and edit the values.

Keep `DRY_RUN=true` for the first tests.

## Run manually

Run the module:

`python -m raspberry_executor.main`

## systemd

A template is available at `systemd/raspberry-executor.service`.

Install it into `/etc/systemd/system/`, reload systemd, enable the service, then start it.

## Safety

The app keeps a local `state.json` file to avoid duplicate execution after restart.

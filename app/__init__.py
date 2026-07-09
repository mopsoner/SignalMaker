"""SignalMaker application package."""

from __future__ import annotations

import os

# Pydantic scans every installed distribution for plugin entry points the first
# time a model is built. On Raspberry Pi installs, unrelated packages can ship
# non-UTF-8 ``entry_points.txt`` metadata, which makes that scan fail before the
# app can import FastAPI. SignalMaker does not use Pydantic plugins, so disable
# plugin discovery unless the operator explicitly opts back in.
os.environ.setdefault("PYDANTIC_DISABLE_PLUGINS", "1")

from app.main import app  # noqa: E402,F401 — allows `gunicorn app:app` auto-detection

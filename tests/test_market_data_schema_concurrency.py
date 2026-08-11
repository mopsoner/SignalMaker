import threading

import pytest

from signalmaker.market_data.repository import MarketDataRepository


class _Dialect:
    name = "postgresql"


class _Bind:
    dialect = _Dialect()


class _NoProcessLock:
    """Expose the database-level protection that separate processes rely on."""

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _PostgresState:
    def __init__(self):
        self.advisory_lock = threading.Lock()
        self.extension_created = False


class _Session:
    def __init__(self, state, extension_error=None):
        self.bind = _Bind()
        self.state = state
        self.extension_error = extension_error
        self.locked = False

    def get_bind(self):
        return self.bind

    def execute(self, statement, _params=None):
        sql = str(statement)
        if "pg_advisory_xact_lock" in sql:
            self.state.advisory_lock.acquire()
            self.locked = True
        elif sql == "CREATE EXTENSION IF NOT EXISTS pgcrypto":
            if self.extension_error:
                raise self.extension_error
            self.state.extension_created = True

    def commit(self):
        if self.locked:
            self.locked = False
            self.state.advisory_lock.release()


def test_ensure_schema_serializes_pgcrypto_install_between_processes(monkeypatch):
    state = _PostgresState()
    repositories = [MarketDataRepository(_Session(state)) for _ in range(2)]
    errors = []

    # A Python lock does not protect API and pipeline processes. Disable it here
    # so this test exercises the PostgreSQL advisory lock instead.
    monkeypatch.setattr(MarketDataRepository, "_schema_lock", _NoProcessLock())
    monkeypatch.setattr(MarketDataRepository, "_schema_ready", set())
    for method in (
        "_ensure_legacy_market_data_schema",
        "_ensure_market_assets_schema",
        "_normalize_ibkr_universes",
        "_ensure_stock_etf_candle_schema",
        "_ensure_analysis_result_indexes",
        "_ensure_run_history_indexes",
    ):
        monkeypatch.setattr(MarketDataRepository, method, lambda *_args: None)

    def run(repository):
        try:
            repository.ensure_schema()
        except Exception as exc:  # pragma: no cover - assertion reports thread errors
            errors.append(exc)

    threads = [threading.Thread(target=run, args=(repository,)) for repository in repositories]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert state.extension_created


def test_pgcrypto_install_does_not_mask_other_sql_errors():
    expected = RuntimeError("permission denied")
    repository = MarketDataRepository(_Session(_PostgresState(), expected))

    with pytest.raises(RuntimeError, match="permission denied"):
        repository._ensure_pgcrypto()


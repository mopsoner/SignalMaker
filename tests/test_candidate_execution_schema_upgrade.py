import asyncio
from types import SimpleNamespace

from app import main
from app.db import base


class _Connection:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(str(statement))


class _Transaction:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, *_):
        return None


def test_compatible_schema_upgrade_adds_candidate_execution_lifecycle_columns(monkeypatch):
    connection = _Connection()
    engine = SimpleNamespace(
        dialect=SimpleNamespace(name="postgresql"),
        begin=lambda: _Transaction(connection),
    )
    monkeypatch.setattr(base, "engine", engine)

    base.apply_compatible_schema_upgrades()

    statements = "\n".join(connection.statements)
    assert "ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ" in statements
    assert "ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMPTZ" in statements
    assert "SET status = 'completed' WHERE status = 'executed'" in statements


def test_startup_applies_compatible_upgrades_when_create_all_is_disabled(monkeypatch):
    calls = []

    class _Session:
        def close(self):
            calls.append("close")

    class _Repository:
        def __init__(self, _db):
            pass

        def ensure_schema(self):
            calls.append("market_schema")

    monkeypatch.setattr(main.settings, "create_tables_on_boot", False)
    monkeypatch.setattr(main, "init_db", lambda: calls.append("create_all"))
    monkeypatch.setattr(
        main, "apply_compatible_schema_upgrades", lambda: calls.append("upgrades")
    )
    monkeypatch.setattr(main, "SessionLocal", _Session)
    monkeypatch.setattr(main, "MarketDataRepository", _Repository)

    async def run_lifespan():
        async with main.lifespan(main.app):
            pass

    asyncio.run(run_lifespan())

    assert calls == ["upgrades", "market_schema", "close"]

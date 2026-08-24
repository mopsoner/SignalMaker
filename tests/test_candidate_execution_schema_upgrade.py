from types import SimpleNamespace

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

    base._apply_compatible_schema_upgrades()

    statements = "\n".join(connection.statements)
    assert "ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ" in statements
    assert "ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMPTZ" in statements
    assert "SET status = 'completed' WHERE status = 'executed'" in statements

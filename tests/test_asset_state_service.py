from unittest.mock import MagicMock

from app.services.asset_state_service import AssetStateService


def _executed_sql(db: MagicMock) -> list[str]:
    return [str(call.args[0]) for call in db.execute.call_args_list]


def test_initialization_without_legacy_rsi_column_skips_copy() -> None:
    db = MagicMock()
    db.execute.return_value.first.return_value = None

    AssetStateService(db)

    statements = _executed_sql(db)
    assert len(statements) == 2
    assert "ALTER TABLE asset_state_current" in statements[0]
    assert "information_schema.columns" in statements[1]
    assert not any("UPDATE asset_state_current" in statement for statement in statements)
    assert db.execute.call_args_list[1].args[1] == {
        "table_name": "asset_state_current",
        "column_name": "rsi_5m",
    }
    db.commit.assert_called_once_with()


def test_initialization_with_legacy_rsi_column_copies_non_null_values() -> None:
    db = MagicMock()
    db.execute.return_value.first.return_value = (1,)

    AssetStateService(db)

    statements = _executed_sql(db)
    assert len(statements) == 3
    assert "information_schema.columns" in statements[1]
    assert "UPDATE asset_state_current" in statements[2]
    assert "SET rsi_15m = rsi_5m" in statements[2]
    assert "WHERE rsi_15m IS NULL" in statements[2]
    assert "AND rsi_5m IS NOT NULL" in statements[2]
    db.commit.assert_called_once_with()

from app.services import runtime_settings


def test_load_admin_settings_omits_display_alias_duplicates(monkeypatch):
    def fake_runtime_settings(db=None):
        return {
            "general": {"admin_token": "token"},
            "executor": {"execution_exchange": "kraken", "quote_assets": "USDC"},
            "kraken": {"kraken_api_key": "kraken-key", "kraken_secret_key": "kraken-secret"},
            "kraken": {
                "kraken_base_url": "https://api.kraken.com",
                "kraken_api_key": "kraken-key",
                "kraken_secret_key": "kraken-secret",
            },
        }

    monkeypatch.setattr(runtime_settings, "load_runtime_settings", fake_runtime_settings)

    payload = runtime_settings.load_admin_settings()

    assert payload["executor"] == {"execution_exchange": "kraken", "quote_assets": "USDC"}
    assert payload["kraken"] == {
        "kraken_base_url": "https://api.kraken.com",
        "kraken_api_key": "kraken-key",
        "kraken_secret_key": "kraken-secret",
    }
    assert "EXECUTION_EXCHANGE" not in payload["kraken"]
    assert "KRAKEN_BASE_URL" not in payload["kraken"]
    assert "KRAKEN_API_KEY" not in payload["kraken"]


def test_load_runtime_settings_canonicalizes_stored_alias_rows(monkeypatch):
    class FakeRow:
        def __init__(self, category, key, value):
            self.category = category
            self.key = key
            self.value = value

    class FakeScalars:
        def all(self):
            return [
                FakeRow("kraken", "EXECUTION_EXCHANGE", "kraken"),
                FakeRow("kraken", "KRAKEN_BASE_URL", "https://kraken.test"),
                FakeRow("kraken", "KRAKEN_API_KEY", "kraken-alias-key"),
                FakeRow("admin/security", "ADMIN_TOKEN", "admin-alias-token"),
            ]

    class FakeResult:
        def scalars(self):
            return FakeScalars()

    class FakeDb:
        def execute(self, statement):
            return FakeResult()

    payload = runtime_settings.load_runtime_settings(FakeDb())

    assert payload["executor"]["execution_exchange"] == "kraken"
    assert payload["kraken"]["kraken_base_url"] == "https://kraken.test"
    assert payload["kraken"]["kraken_api_key"] == "kraken-alias-key"
    assert payload["general"]["admin_token"] == "admin-alias-token"
    assert "EXECUTION_EXCHANGE" not in payload["kraken"]
    assert "KRAKEN_BASE_URL" not in payload["kraken"]
    assert "KRAKEN_API_KEY" not in payload["kraken"]


def test_normalize_admin_payload_uppercase_kraken_keys():
    payload = runtime_settings._normalize_admin_payload(
        {"kraken": {"KRAKEN_API_KEY": "key", "KRAKEN_SECRET_KEY": "secret", "KRAKEN_BASE_URL": "https://k"}}
    )

    assert payload["kraken"]["kraken_api_key"] == "key"
    assert payload["kraken"]["kraken_secret_key"] == "secret"
    assert payload["kraken"]["kraken_base_url"] == "https://k"
    assert "KRAKEN_API_KEY" not in payload["kraken"]


def test_legacy_alias_rows_do_not_override_canonical_values():
    class FakeRow:
        def __init__(self, category, key, value):
            self.category = category
            self.key = key
            self.value = value

    class FakeScalars:
        def all(self):
            return [
                FakeRow("kraken", "kraken_api_key", "real-api-key"),
                FakeRow("kraken", "kraken_secret_key", "real-secret-key"),
                FakeRow("kraken", "KRAKEN_API_KEY", "bad"),
                FakeRow("kraken", "KRAKEN_SECRET_KEY", "bad"),
            ]

    class FakeResult:
        def scalars(self):
            return FakeScalars()

    class FakeDb:
        def execute(self, statement):
            return FakeResult()

    payload = runtime_settings.load_runtime_settings(FakeDb())

    assert payload["kraken"]["kraken_api_key"] == "real-api-key"
    assert payload["kraken"]["kraken_secret_key"] == "real-secret-key"


def test_load_admin_settings_returns_curated_sections_with_empty_defaults(monkeypatch):
    def fake_runtime_settings(db=None):
        return {
            "general": {"app_name": "SignalMaker", "admin_token": "secret"},
            "executor": {"execution_exchange": "kraken", "quote_assets": "USDC"},
            "kraken": {"kraken_base_url": "https://api.kraken.com", "kraken_api_key": "key", "kraken_secret_key": None},
            "market_data": {"kraken_max_symbols": 25},
            "strategy": {"planner_min_score": 4},
            "notifications": {"telegram_chat_id": "chat-1", "telegram_secret": None},
            "bot": {"bot_executor_interval_sec": 45},
            "live": {"kraken_use_testnet": False, "live_reconcile_enabled": True},
        }

    monkeypatch.setattr(runtime_settings, "load_runtime_settings", fake_runtime_settings)

    payload = runtime_settings.load_admin_settings()

    assert set(payload) == set(runtime_settings.ADMIN_EDITABLE_FIELDS)
    for section, keys in runtime_settings.ADMIN_EDITABLE_FIELDS.items():
        assert set(payload[section]) == set(keys)

    assert payload["general"] == {"admin_token": "secret"}
    assert "app_name" not in payload["general"]
    assert payload["executor"]["quote_assets"] == "USDC"
    assert payload["kraken"]["kraken_api_key"] == "key"
    assert payload["kraken"]["kraken_secret_key"] == ""
    assert payload["market_data"]["kraken_max_symbols"] == 25
    assert payload["market_data"]["kraken_quote_assets"] == runtime_settings.DEFAULT_SETTINGS["market_data"]["kraken_quote_assets"]
    assert payload["strategy"]["planner_min_score"] == 4
    assert payload["notifications"]["telegram_chat_id"] == "chat-1"
    assert payload["notifications"]["telegram_secret"] == ""
    assert payload["bot"]["bot_executor_interval_sec"] == 45
    assert payload["live"]["kraken_use_testnet"] is False
    assert payload["live"]["live_reconcile_enabled"] is True
    assert "kraken_quote_assets" not in payload["kraken"]
    assert "EXECUTION_EXCHANGE" not in payload["executor"]
    assert "KRAKEN_BASE_URL" not in payload["kraken"]


def test_migrate_bootstrap_settings_to_app_settings_fills_missing_canonical_rows(monkeypatch):
    added = []

    class FakeScalars:
        def all(self):
            return added

    class FakeResult:
        def scalars(self):
            return FakeScalars()

    class FakeDb:
        def add(self, row):
            added.append(row)

        def commit(self):
            pass

        def execute(self, statement):
            return FakeResult()

    monkeypatch.setattr(
        runtime_settings,
        "_legacy_bootstrap_values",
        lambda: {"KRAKEN_BASE_URL": "https://bootstrap.kraken", "QUOTE_ASSETS": "USD,EUR"},
    )

    rows = runtime_settings.migrate_bootstrap_settings_to_app_settings(FakeDb(), [])

    values = {(row.category, row.key): row.value for row in rows}
    assert values[("kraken", "kraken_base_url")] == "https://bootstrap.kraken"
    assert values[("executor", "quote_assets")] == "USD,EUR"


def test_migrate_bootstrap_settings_to_app_settings_does_not_overwrite_canonical(monkeypatch):
    canonical = runtime_settings.AppSetting(category="kraken", key="kraken_base_url", value="https://canonical.kraken")

    class FakeDb:
        def add(self, row):
            raise AssertionError("canonical row should not be recreated")

        def commit(self):
            raise AssertionError("unchanged canonical rows should not commit")

    monkeypatch.setattr(
        runtime_settings,
        "_legacy_bootstrap_values",
        lambda: {"KRAKEN_BASE_URL": "https://bootstrap.kraken"},
    )

    rows = runtime_settings.migrate_bootstrap_settings_to_app_settings(FakeDb(), [canonical])

    assert rows[0].value == "https://canonical.kraken"


def test_migrate_legacy_kraken_base_url_rows_renames_and_deletes_old_key():
    legacy = runtime_settings.AppSetting(category="kraken", key="kraken_rest_base", value="https://legacy.kraken")
    added = []
    deleted = []

    class FakeScalars:
        def all(self):
            return added

    class FakeResult:
        def scalars(self):
            return FakeScalars()

    class FakeDb:
        def add(self, row):
            added.append(row)

        def delete(self, row):
            deleted.append(row)

        def commit(self):
            pass

        def execute(self, statement):
            return FakeResult()

    rows = runtime_settings._migrate_legacy_kraken_base_url_rows(FakeDb(), [legacy])

    assert [(row.category, row.key, row.value) for row in rows] == [
        ("kraken", "kraken_base_url", "https://legacy.kraken")
    ]
    assert deleted == [legacy]


def test_migrate_legacy_kraken_base_url_rows_keeps_existing_canonical_value():
    canonical = runtime_settings.AppSetting(category="kraken", key="kraken_base_url", value="https://canonical.kraken")
    legacy = runtime_settings.AppSetting(category="kraken", key="kraken_rest_base", value="https://legacy.kraken")
    deleted = []

    class FakeScalars:
        def all(self):
            return [canonical]

    class FakeResult:
        def scalars(self):
            return FakeScalars()

    class FakeDb:
        def add(self, row):
            raise AssertionError("canonical row should not be recreated")

        def delete(self, row):
            deleted.append(row)

        def commit(self):
            pass

        def execute(self, statement):
            return FakeResult()

    rows = runtime_settings._migrate_legacy_kraken_base_url_rows(FakeDb(), [canonical, legacy])

    assert rows == [canonical]
    assert canonical.value == "https://canonical.kraken"
    assert deleted == [legacy]


def test_cleanup_legacy_app_settings_backs_up_then_deletes_legacy_rows(tmp_path):
    canonical = runtime_settings.AppSetting(category="kraken", key="kraken_base_url", value="https://canonical.kraken")
    legacy = runtime_settings.AppSetting(category="kraken", key="kraken_rest_base", value="https://legacy.kraken")
    alias = runtime_settings.AppSetting(category="kraken", key="KRAKEN_API_KEY", value="legacy-key")
    rows = [canonical, legacy, alias]
    deleted = []

    class FakeScalars:
        def all(self):
            return rows

    class FakeResult:
        def scalars(self):
            return FakeScalars()

    class FakeDb:
        def execute(self, statement):
            return FakeResult()

        def delete(self, row):
            deleted.append(row)
            rows.remove(row)

        def commit(self):
            pass

    result = runtime_settings.cleanup_legacy_app_settings(FakeDb(), tmp_path / "backup.json")

    assert result["deleted_count"] == 2
    assert result["deleted_keys"] == ["kraken.KRAKEN_API_KEY", "kraken.kraken_rest_base"]
    assert deleted == [alias, legacy] or deleted == [legacy, alias]
    assert rows == [canonical]
    assert '"critical_rows"' in (tmp_path / "backup.json").read_text()
    assert "https://legacy.kraken" in (tmp_path / "backup.json").read_text()

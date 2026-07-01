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
        "kraken_api_key": {"configured": True},
        "kraken_secret_key": {"configured": True},
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
    assert payload["kraken"]["kraken_api_key"] == {"configured": True}
    assert payload["kraken"]["kraken_secret_key"] == {"configured": False}
    assert payload["market_data"]["kraken_max_symbols"] == 25
    assert payload["market_data"]["kraken_quote_assets"] == runtime_settings.DEFAULT_SETTINGS["market_data"]["kraken_quote_assets"]
    assert payload["strategy"]["planner_min_score"] == 4
    assert payload["notifications"]["telegram_chat_id"] == "chat-1"
    assert payload["notifications"]["telegram_secret"] == {"configured": False}
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


def test_bootstrap_env_aliases_cover_runtime_default_fields():
    expected_aliases = {
        "KRAKEN_COLLECTOR_ENABLED": ("market_data", "kraken_collector_enabled"),
        "KRAKEN_SYMBOL_STATUS": ("market_data", "kraken_symbol_status"),
        "KRAKEN_MAX_SYMBOLS": ("market_data", "kraken_max_symbols"),
        "KRAKEN_COLLECT_MAX_WORKERS": ("market_data", "kraken_collect_max_workers"),
        "KRAKEN_INCREMENTAL_FETCH_ENABLED": ("market_data", "kraken_incremental_fetch_enabled"),
        "KRAKEN_INCREMENTAL_MIN_1M": ("market_data", "kraken_incremental_min_1m"),
        "KRAKEN_LOOKBACK_4H": ("market_data", "kraken_lookback_4h"),
        "LIVE_REQUIRE_TP_SL": ("live", "live_require_tp_sl"),
        "LIVE_RECONCILE_ENABLED": ("live", "live_reconcile_enabled"),
        "MOMENTUM_CANDIDATES_MIN_RR": ("momentum", "momentum_candidates_min_rr"),
        "MOMENTUM_CANDIDATES_REQUIRE_WYCKOFF_CONTEXT": (
            "momentum",
            "momentum_candidates_require_wyckoff_context",
        ),
        "MOMENTUM_CANDIDATES_HTTP_TIMEOUT_SEC": ("momentum", "momentum_candidates_http_timeout_sec"),
        "MOMENTUM_CANDIDATES_SOURCE_PATH": ("momentum", "momentum_candidates_source_path"),
        "MOMENTUM_CANDIDATES_TARGET_PCT": ("momentum", "momentum_candidates_target_pct"),
        "SIGNAL_RSI_PERIOD": ("strategy", "signal_rsi_period"),
        "SIGNAL_SESSION_CONFIRM_FILTER_ENABLED": ("strategy", "signal_session_confirm_filter_enabled"),
        "PLANNER_MIN_SCORE": ("strategy", "planner_min_score"),
        "PLANNER_MIN_RR": ("strategy", "planner_min_rr"),
        "BOT_EXECUTOR_LIMIT": ("bot", "bot_executor_limit"),
        "BOT_EXECUTOR_QUANTITY": ("bot", "bot_executor_quantity"),
    }

    for env_key, target in expected_aliases.items():
        assert runtime_settings.BOOTSTRAP_ENV_ALIASES[env_key] == target
        assert runtime_settings.LEGACY_RASPBERRY_SETTING_ALIASES[env_key] == target

    assert runtime_settings.BOOTSTRAP_ENV_ALIASES["KRAKEN_QUOTE_ASSETS"] == ("executor", "quote_assets")


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


def test_migrate_bootstrap_settings_to_app_settings_fills_empty_canonical_rows(monkeypatch):
    canonical = runtime_settings.AppSetting(category="live", key="live_require_tp_sl", value="")

    class FakeScalars:
        def all(self):
            return [canonical]

    class FakeResult:
        def scalars(self):
            return FakeScalars()

    class FakeDb:
        def add(self, row):
            raise AssertionError("empty canonical row should be updated, not recreated")

        def commit(self):
            pass

        def execute(self, statement):
            return FakeResult()

    monkeypatch.setattr(
        runtime_settings,
        "_legacy_bootstrap_values",
        lambda: {"LIVE_REQUIRE_TP_SL": "false"},
    )

    rows = runtime_settings.migrate_bootstrap_settings_to_app_settings(FakeDb(), [canonical])

    assert rows[0].value == "false"


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


def test_admin_settings_sources_prefer_canonical_db_over_bootstrap_env(monkeypatch):
    canonical = runtime_settings.AppSetting(
        category="kraken", key="kraken_base_url", value="https://db.kraken"
    )

    class FakeScalars:
        def all(self):
            return [canonical]

    class FakeResult:
        def scalars(self):
            return FakeScalars()

    class FakeDb:
        def execute(self, statement):
            return FakeResult()

    monkeypatch.setattr(
        runtime_settings,
        "_legacy_bootstrap_values",
        lambda: {"KRAKEN_BASE_URL": "https://env.kraken"},
    )

    payload = runtime_settings.load_admin_settings(FakeDb(), include_sources=True)

    assert payload["settings"]["kraken"]["kraken_base_url"] == "https://db.kraken"
    assert payload["sources"]["kraken"]["kraken_base_url"] == "db"


def test_load_admin_settings_sources_mark_base_settings_dotenv_keys(monkeypatch, tmp_path):
    env_keys = {
        "KRAKEN_MAX_SYMBOLS": "15",
        "KRAKEN_BASE_URL": "https://dotenv.kraken.test",
        "LIVE_TRADING_ENABLED": "true",
        "LIVE_MAX_OPEN_POSITIONS": "3",
        "BOT_PIPELINE_ENABLED": "false",
        "BOT_EXECUTOR_LIMIT": "7",
        "SIGNAL_RSI_PERIOD": "21",
        "SIGNAL_OVERBOUGHT": "75",
    }
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("KRAKEN_MAX_SYMBOLS", raising=False)
    monkeypatch.delenv("KRAKEN_BASE_URL", raising=False)
    monkeypatch.delenv("LIVE_TRADING_ENABLED", raising=False)
    monkeypatch.delenv("LIVE_MAX_OPEN_POSITIONS", raising=False)
    monkeypatch.delenv("BOT_PIPELINE_ENABLED", raising=False)
    monkeypatch.delenv("BOT_EXECUTOR_LIMIT", raising=False)
    monkeypatch.delenv("SIGNAL_RSI_PERIOD", raising=False)
    monkeypatch.delenv("SIGNAL_OVERBOUGHT", raising=False)
    (tmp_path / ".env").write_text("\n".join(f"{key}={value}" for key, value in env_keys.items()) + "\n")
    runtime_settings.settings_env_file_keys.cache_clear()

    class FakeScalars:
        def all(self):
            return []

    class FakeResult:
        def scalars(self):
            return FakeScalars()

    class FakeDb:
        def execute(self, statement):
            return FakeResult()

    try:
        payload = runtime_settings.load_admin_settings(FakeDb(), include_sources=True)
    finally:
        runtime_settings.settings_env_file_keys.cache_clear()

    assert payload["sources"]["market_data"]["kraken_max_symbols"] == ".env/bootstrap"
    assert payload["sources"]["kraken"]["kraken_base_url"] == ".env/bootstrap"
    assert payload["sources"]["live"]["live_trading_enabled"] == ".env/bootstrap"
    assert payload["sources"]["live"]["live_max_open_positions"] == ".env/bootstrap"
    assert payload["sources"]["bot"]["bot_pipeline_enabled"] == ".env/bootstrap"
    assert payload["sources"]["bot"]["bot_executor_limit"] == ".env/bootstrap"
    assert payload["sources"]["strategy"]["signal_rsi_period"] == ".env/bootstrap"
    assert payload["sources"]["strategy"]["signal_overbought"] == ".env/bootstrap"

def test_load_admin_settings_masks_sensitive_values(monkeypatch):
    def fake_runtime_settings(db=None):
        return {
            "kraken": {"kraken_api_key": "kraken-key", "kraken_secret_key": "kraken-secret"},
            "notifications": {"telegram_secret": "telegram-token", "discord_url": "https://discord.test/hook"},
        }

    monkeypatch.setattr(runtime_settings, "load_runtime_settings", fake_runtime_settings)

    payload = runtime_settings.load_admin_settings()

    assert payload["kraken"]["kraken_api_key"] == {"configured": True}
    assert payload["kraken"]["kraken_secret_key"] == {"configured": True}
    assert payload["notifications"]["telegram_secret"] == {"configured": True}
    assert payload["notifications"]["discord_url"] == {"configured": True}
    assert "kraken-key" not in str(payload)
    assert "kraken-secret" not in str(payload)
    assert "telegram-token" not in str(payload)
    assert "discord.test" not in str(payload)


def test_persist_runtime_settings_keeps_existing_secrets_for_empty_or_masked_payloads(monkeypatch):
    rows = {
        ("kraken", "kraken_api_key"): runtime_settings.AppSetting(category="kraken", key="kraken_api_key", value="existing-key"),
        ("kraken", "kraken_secret_key"): runtime_settings.AppSetting(category="kraken", key="kraken_secret_key", value="existing-secret"),
        ("notifications", "telegram_secret"): runtime_settings.AppSetting(category="notifications", key="telegram_secret", value="existing-telegram"),
        ("notifications", "discord_url"): runtime_settings.AppSetting(category="notifications", key="discord_url", value="existing-discord"),
    }
    added = []

    class FakeResult:
        def __init__(self, row=None):
            self.row = row

        def scalar_one_or_none(self):
            return self.row

        def scalars(self):
            return self

        def all(self):
            return list(rows.values()) + added

    class FakeDb:
        def execute(self, statement):
            try:
                criteria = statement.whereclause
                category = criteria.left.value if criteria.left.name == "category" else criteria.right.value
                key_clause = criteria.right
                key = key_clause.right.value if hasattr(key_clause, "right") else None
                if key is None:
                    text_statement = str(statement)
                    return FakeResult()
                return FakeResult(rows.get((category, key)))
            except Exception:
                return FakeResult()

        def add(self, row):
            added.append(row)
            rows[(row.category, row.key)] = row

        def commit(self):
            pass

        def delete(self, row):
            pass

    monkeypatch.setattr(runtime_settings, "load_runtime_settings", lambda db: {"ok": {}})

    runtime_settings.persist_runtime_settings(
        FakeDb(),
        {
            "kraken": {"kraken_api_key": "", "kraken_secret_key": "********"},
            "notifications": {"telegram_secret": {"configured": True}, "discord_url": "••••••••"},
        },
    )

    assert rows[("kraken", "kraken_api_key")].value == "existing-key"
    assert rows[("kraken", "kraken_secret_key")].value == "existing-secret"
    assert rows[("notifications", "telegram_secret")].value == "existing-telegram"
    assert rows[("notifications", "discord_url")].value == "existing-discord"


def test_coerce_bool_accepts_common_db_and_admin_values():
    assert runtime_settings._coerce_bool("false", default=True) is False
    assert runtime_settings._coerce_bool("true") is True
    assert runtime_settings._coerce_bool(False, default=True) is False
    assert runtime_settings._coerce_bool(True) is True
    assert runtime_settings._coerce_bool("0", default=True) is False
    assert runtime_settings._coerce_bool("1") is True
    assert runtime_settings._coerce_bool("yes") is True
    assert runtime_settings._coerce_bool("no", default=True) is False
    assert runtime_settings._coerce_bool("on") is True
    assert runtime_settings._coerce_bool("off", default=True) is False
    assert runtime_settings._coerce_bool("unexpected", default=True) is True


def test_load_runtime_settings_coerces_boolean_fields_from_db_values():
    rows = [
        runtime_settings.AppSetting(category="market_data", key="kraken_collector_enabled", value="false"),
        runtime_settings.AppSetting(category="market_data", key="kraken_incremental_fetch_enabled", value="0"),
        runtime_settings.AppSetting(category="momentum", key="momentum_candidates_sync_enabled", value="true"),
        runtime_settings.AppSetting(category="momentum", key="momentum_candidates_require_wyckoff_context", value="1"),
        runtime_settings.AppSetting(category="live", key="live_trading_enabled", value=False),
        runtime_settings.AppSetting(category="live", key="kraken_use_testnet", value=True),
        runtime_settings.AppSetting(category="bot", key="bot_pipeline_enabled", value="false"),
        runtime_settings.AppSetting(category="bot", key="bot_executor_enabled", value="true"),
        runtime_settings.AppSetting(category="bot", key="bot_scheduler_enabled", value="0"),
    ]

    class FakeScalars:
        def all(self):
            return rows

    class FakeResult:
        def scalars(self):
            return FakeScalars()

    class FakeDb:
        def execute(self, statement):
            return FakeResult()

    payload = runtime_settings.load_runtime_settings(FakeDb())

    assert payload["market_data"]["kraken_collector_enabled"] is False
    assert payload["market_data"]["kraken_incremental_fetch_enabled"] is False
    assert payload["momentum"]["momentum_candidates_sync_enabled"] is True
    assert payload["momentum"]["momentum_candidates_require_wyckoff_context"] is True
    assert payload["live"]["live_trading_enabled"] is False
    assert payload["live"]["kraken_use_testnet"] is True
    assert payload["bot"]["bot_pipeline_enabled"] is False
    assert payload["bot"]["bot_executor_enabled"] is True
    assert payload["bot"]["bot_scheduler_enabled"] is False


def test_signal_execution_interval_uses_runtime_value():
    rows = [runtime_settings.AppSetting(category="strategy", key="signal_execution_interval", value="1h")]

    class FakeScalars:
        def all(self):
            return rows

    class FakeResult:
        def scalars(self):
            return FakeScalars()

    class FakeDb:
        def execute(self, statement):
            return FakeResult()

    payload = runtime_settings.load_runtime_settings(FakeDb())
    config = runtime_settings.get_runtime_signal_config(FakeDb())

    assert payload["strategy"]["signal_execution_interval"] == "1h"
    assert config["execution_interval"] == "1h"

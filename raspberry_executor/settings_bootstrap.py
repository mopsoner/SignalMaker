from raspberry_executor.env_store import read_env, write_env
from raspberry_executor.settings_store import read_settings, write_settings
from raspberry_executor.sqlite_db import connect, init_db, now_iso


MIGRATION_KEY = "dry_run_default_false_migrated"


def _migration_done() -> bool:
    try:
        init_db()
        with connect() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key=?", (MIGRATION_KEY,)).fetchone()
            return bool(row and str(row["value"]).lower() == "true")
    except Exception:
        return False


def _mark_migration_done() -> None:
    try:
        init_db()
        with connect() as conn:
            conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, 'true')", (MIGRATION_KEY,))
            conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES('dry_run_default_false_migrated_at', ?)", (now_iso(),))
            conn.commit()
    except Exception:
        pass


def _apply_dry_run_default_false_once(values: dict[str, str]) -> tuple[dict[str, str], bool]:
    if _migration_done():
        return values, False
    migrated = dict(values)
    migrated["DRY_RUN"] = "false"
    _mark_migration_done()
    return migrated, True


def bootstrap_settings() -> dict:
    """Compatibility bootstrap for the deprecated local settings table.

    Runtime settings are now canonical in app_settings. This function only keeps
    older Raspberry installs bootable long enough for their local .env/SQLite
    values to be migrated into app_settings by app.services.runtime_settings.
    """
    env_values = read_env()
    stored_values = read_settings()
    if stored_values:
        merged = {**env_values, **stored_values}
        merged, dry_run_migrated = _apply_dry_run_default_false_once(merged)
        write_settings(merged)
        write_env(merged)
        return {"status": "restored_env_from_settings", "settings_count": len(stored_values), "dry_run_migrated": dry_run_migrated, "dry_run": merged.get("DRY_RUN")}
    env_values, dry_run_migrated = _apply_dry_run_default_false_once(env_values)
    write_settings(env_values)
    write_env(env_values)
    return {"status": "seeded_settings_from_env", "settings_count": len(env_values), "dry_run_migrated": dry_run_migrated, "dry_run": env_values.get("DRY_RUN")}

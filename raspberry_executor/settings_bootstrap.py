from raspberry_executor.env_store import read_env, write_env
from raspberry_executor.settings_store import read_settings, write_settings


def bootstrap_settings() -> dict:
    """Keep local .env and SQLite settings in sync.

    Rules:
    - If SQLite settings already exist, they restore .env at startup.
    - If SQLite settings is empty, current .env seeds the settings table.
    - Admin saves should write both .env and SQLite settings.
    """
    env_values = read_env()
    stored_values = read_settings()
    if stored_values:
        merged = {**env_values, **stored_values}
        write_env(merged)
        return {"status": "restored_env_from_settings", "settings_count": len(stored_values)}
    write_settings(env_values)
    return {"status": "seeded_settings_from_env", "settings_count": len(env_values)}

import json

from raspberry_executor.sqlite_db import DB_PATH, init_db, migrate_state_json_once


def main() -> None:
    init_db()
    migration = migrate_state_json_once()
    print(json.dumps({"status": "ok", "db_path": str(DB_PATH), "migration": migration}, indent=2))


if __name__ == "__main__":
    main()

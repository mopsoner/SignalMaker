from app.db.base import init_db
from app.db.session import SessionLocal
from app.services.runtime_settings import seed_app_settings_from_env


if __name__ == "__main__":
    init_db()
    db = SessionLocal()
    try:
        summary = seed_app_settings_from_env(db)
    finally:
        db.close()
    print("SignalMaker DB initialized")
    print(
        "app_settings seed: "
        f"created={len(summary['created'])} "
        f"filled_empty={len(summary['filled_empty'])} "
        f"kept_db={len(summary['kept_db'])}"
    )

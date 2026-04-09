from app.db.session import engine
from app.models.base import Base
from app.models.asset_state import AssetStateCurrent


def init_db() -> None:
    Base.metadata.create_all(bind=engine)

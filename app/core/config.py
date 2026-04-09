from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = Field(default="SignalMaker", alias="APP_NAME")
    app_env: str = Field(default="development", alias="APP_ENV")
    app_host: str = Field(default="0.0.0.0", alias="APP_HOST")
    app_port: int = Field(default=8080, alias="APP_PORT")
    database_url: str = Field(default="sqlite:///./signalmaker.db", alias="DATABASE_URL")
    sql_echo: bool = Field(default=False, alias="SQL_ECHO")
    create_tables_on_boot: bool = Field(default=True, alias="CREATE_TABLES_ON_BOOT")
    cors_origins: str = Field(default="http://localhost:3000,http://localhost:8080", alias="CORS_ORIGINS")

    @property
    def cors_origin_list(self) -> list[str]:
        return [item.strip() for item in self.cors_origins.split(",") if item.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

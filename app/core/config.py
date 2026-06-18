from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Keep the field names identical to the environment variable names. That
    # makes settings usage explicit at call sites and avoids alias indirection in
    # a small service.
    ANTHROPIC_API_KEY: str = ""
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/freight_ap"
    TEST_DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/freight_ap_test"
    GMAIL_CREDENTIALS_PATH: str = "/app/credentials.json"
    GMAIL_TOKEN_PATH: str = "/app/token.json"
    REDIS_URL: str = "redis://localhost:6379/0"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    # Settings are cached so each request does not repeatedly parse .env. Tests
    # can clear the cache if they need to swap environment variables.
    return Settings()

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
    INVOICE_STREAM: str = "freight-ap:invoice-jobs:v1"
    INVOICE_CONSUMER_GROUP: str = "freight-ap:invoice-workers:v1"
    INVOICE_DEAD_LETTER_STREAM: str = "freight-ap:invoice-jobs:dlq:v1"
    INVOICE_DEDUPE_PREFIX: str = "freight-ap:invoice-dedupe"
    INVOICE_METADATA_PREFIX: str = "freight-ap:invoice-meta"
    INVOICE_MAX_ATTEMPTS: int = 3
    INVOICE_VISIBILITY_TIMEOUT_MS: int = 300_000
    INVOICE_READ_BLOCK_MS: int = 5_000
    INVOICE_DEDUPE_TTL_SECONDS: int = 30 * 24 * 60 * 60
    # A bounded fan-out limits simultaneous Claude requests, SQLAlchemy
    # sessions, and per-document LangGraph checkpointer connections.
    MAX_BATCH_SIZE: int = 10

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

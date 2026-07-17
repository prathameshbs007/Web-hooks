from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All runtime configuration. Values come from env vars (see .env.example)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://relay:relay@postgres:5432/relay"
    redis_url: str = "redis://redis:6379/0"

    admin_token: str = "change-me"

    anthropic_api_key: str = ""
    agent_model: str = "claude-sonnet-4-6"
    agent_max_runs_per_endpoint_per_hour: int = 1
    agent_max_runs_per_day: int = 10

    worker_concurrency: int = 32
    stream_shards: int = 8
    default_tenant_rate_per_sec: int = 50
    default_tenant_max_inflight: int = 20
    delivery_timeout_seconds: int = 10
    max_attempts: int = 7


@lru_cache
def get_settings() -> Settings:
    return Settings()

"""Application configuration loaded from environment variables."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "sqlite+aiosqlite:///./insight.db"
    youtube_api_key: str = ""
    youtube_channel_id: str = ""
    youtube_refresh_interval_minutes: int = 15
    # Optional — enables chart backfill via YouTube Analytics API
    youtube_client_id: str = ""
    youtube_client_secret: str = ""
    youtube_refresh_token: str = ""


settings = Settings()

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Telegram Bot
    BOT_TOKEN: str

    # Telegram MTProto (Telethon — for parsing)
    API_ID: int
    API_HASH: str
    TELEGRAM_PHONE: str
    TELEGRAM_SESSION_NAME: str = "bot_parser_session"

    # Database
    # Bot process uses asyncpg; Celery worker uses psycopg2
    DATABASE_URL_ASYNC: str  # postgresql+asyncpg://...
    DATABASE_URL_SYNC: str   # postgresql://...

    # Redis / Celery
    REDIS_URL: str = "redis://redis:6379/0"
    CELERY_BROKER_URL: str = "redis://redis:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://redis:6379/1"

    # Credit system (unified — no free/paid distinction at storage level)
    INITIAL_CREDITS: int = 5
    PAID_PARSE_COUNT: int = 4
    STARS_PRICE: int = 100  # Telegram Stars

    # Stale job thresholds (separate because processing/pending have different expected durations)
    STALE_PROCESSING_MINUTES: int = 10  # job stuck in 'processing'
    STALE_PENDING_MINUTES: int = 5      # job stuck in 'pending' (Celery never picked it up)

    # Environment
    ENVIRONMENT: str = "development"
    DEBUG: bool = False


settings = Settings()

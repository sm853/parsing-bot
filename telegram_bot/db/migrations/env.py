"""
Alembic migration environment for the telegram_bot module.
Covers only the bot-specific tables (bot_users, parse_jobs, post_results, commenter_results).
The existing backend tables (users, channels, posts, comments) are managed separately.
"""

import os
import sys
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# Ensure the project root is on sys.path so telegram_bot imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from telegram_bot.db.models import Base  # noqa: E402

# Alembic Config object — gives access to values in alembic.ini
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Only autogenerate against the bot models' metadata
target_metadata = Base.metadata


def get_url() -> str:
    """Prefer DATABASE_URL_SYNC env var; fall back to alembic.ini sqlalchemy.url."""
    return os.environ.get("DATABASE_URL_SYNC") or config.get_main_option("sqlalchemy.url")


def run_migrations_offline() -> None:
    """Offline mode: emit SQL without a live DB connection."""
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Online mode: connect to DB and run migrations directly."""
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

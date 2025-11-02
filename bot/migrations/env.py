import asyncio
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging (guard None to satisfy type checker).
cfg_name = getattr(config, "config_file_name", None)
if cfg_name:  # pragma: no branch
    try:
        fileConfig(cfg_name)
    except Exception:  # pragma: no cover - best effort
        pass

# add your model's MetaData object here
# for 'autogenerate' support
import importlib

# ruff: noqa: E402
# importlib usage is local to alembic autogeneration; keep at top to satisfy linters
try:
    models = importlib.import_module("bot.app.domain.models")
    target_metadata = models.Base.metadata
except Exception:
    target_metadata = None


from typing import Optional


def get_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL environment variable is required for migrations (e.g. postgresql+asyncpg://user:pass@db:5432/dbname)"
        )
    return url


def run_migrations_offline() -> None:
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    connectable = create_async_engine(
        get_url(),
        pool_size=int(os.environ.get("DB_POOL_SIZE", "5")),
        max_overflow=int(os.environ.get("DB_MAX_OVERFLOW", "10")),
        pool_timeout=int(os.environ.get("DB_POOL_TIMEOUT", "30")),
        future=True,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())

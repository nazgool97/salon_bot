from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context

# -------------------------
# Alembic Config
# -------------------------
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# -------------------------
# Import your models metadata
# -------------------------
from bot.app.domain.models import Base
target_metadata = Base.metadata


# -------------------------
# Run migrations in offline mode
# -------------------------
def run_migrations_offline() -> None:
    """Run migrations without DB connection (generate SQL only)."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


# bot/migrations/env.py  ← замени полностью функцию run_migrations_online

def run_migrations_online() -> None:
    """Run migrations with sync DB engine."""
    import os
    import re

    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise RuntimeError(
            "\033[91mОШИБКА: Переменная DATABASE_URL не найдена!\n"
            "Добавьте в .env файл строку:\n"
            "DATABASE_URL=postgresql+asyncpg://app_user:change_me@db:5432/salon_db\033[0m"
        )

    # If user provided an async URL (postgresql+asyncpg://...), convert it to a
    # sync URL (postgresql://...) for Alembic so migrations run synchronously.
    sync_url = re.sub(r'^(postgresql)\+[^:]+', r'\1', database_url)

    # Force Alembic to use the (possibly converted) sync URL
    config.set_main_option("sqlalchemy.url", sync_url)

    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
        )

        with context.begin_transaction():
            context.run_migrations()


# -------------------------
# Entrypoint
# -------------------------
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
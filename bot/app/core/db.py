"""Async SQLAlchemy database helpers.

Single authoritative module providing:
    * get_engine / get_session / get_session_factory
    * init_db(force=..., on_create=...)
    * _reset_engine_for_tests (used in test isolation)
    * get_db (wrapper for dependency injection)
"""

import os
from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from ..domain.models import Base


# =====================================================
# 🔧 ENV + Static configuration
# =====================================================
DATABASE_URL_ENV = "DATABASE_URL"
DEFAULT_URL = "postgresql+asyncpg://app_user:change_me@db:5432/booking_app"

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None
_SCHEMA_READY: bool = False
_SCHEMA_CHECKING: bool = False




# =====================================================
# ⚙️ Engine / Session factory
# =====================================================
def _make_engine(url: str) -> AsyncEngine:
    """Create an async engine."""
    return create_async_engine(url, echo=False, future=True)


def get_engine() -> AsyncEngine:
    global _engine, _session_factory
    if _engine is None:
        url = os.getenv(DATABASE_URL_ENV, DEFAULT_URL)
        _engine = _make_engine(url)
        _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    get_engine()
    assert _session_factory is not None
    return _session_factory


@asynccontextmanager
async def get_session() -> AsyncIterator[AsyncSession]:
    """Provide a new AsyncSession."""
    global _SCHEMA_READY, _SCHEMA_CHECKING
    if not _SCHEMA_READY and not _SCHEMA_CHECKING:
        _SCHEMA_CHECKING = True
        try:
            eng = get_engine()
            async with eng.begin() as conn:
                from sqlalchemy import text
                try:
                    await conn.execute(text("SELECT 1 FROM users LIMIT 1"))
                    _SCHEMA_READY = True
                except Exception:
                    try:
                        await init_db(force=False)
                    except Exception:
                        pass
        finally:
            _SCHEMA_CHECKING = False

    factory = get_session_factory()
    session = factory()
    try:
        yield session
    finally:
        await session.close()


# =====================================================
# 🧩 DB Init / Reset helpers
# =====================================================
async def init_db(
    force: bool = False, on_create: Callable[[AsyncEngine], None] | None = None
) -> None:
    """Create database schema."""
    engine = get_engine()
    async with engine.begin() as conn:
        if force:
            await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    if on_create:
        on_create(engine)
    global _SCHEMA_READY
    _SCHEMA_READY = True


def _reset_engine_for_tests() -> None:
    """Reset engine references (fast, synchronous)."""
    global _engine, _session_factory, _SCHEMA_READY, _SCHEMA_CHECKING
    _engine = None
    _session_factory = None
    _SCHEMA_READY = False
    _SCHEMA_CHECKING = False


# Role checks were moved to bot.app.telegram.common.roles per SRP. See that module
# for env-backed and DB-backed role helpers.


# =====================================================
# 💡 Dependency-compatible alias
# =====================================================
async def get_db() -> AsyncIterator[AsyncSession]:
    """Wrapper for dependency injection (used in routers)."""
    async with get_session() as session:
        yield session


# =====================================================
# 📦 Export
# =====================================================
__all__ = [
    "get_engine",
    "get_session",
    "get_session_factory",
    "init_db",
    "_reset_engine_for_tests",
    "get_db",
    # role checks moved to bot.app.telegram.common.roles
]

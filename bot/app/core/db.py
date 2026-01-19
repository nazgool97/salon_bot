"""Async SQLAlchemy database helpers.

Single authoritative module providing:
    * get_engine / get_session / get_session_factory
    * init_db(force=..., on_create=...)
    * _reset_engine_for_tests (used in test isolation)
    * get_db (wrapper for dependency injection)
"""

import os
from contextlib import asynccontextmanager
import logging
import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any
from sqlalchemy import event
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

# Schema init flags used by tests / bootstrapping
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
        # Attach lightweight pool event listeners so we can trace checkouts/checkins
        # and identify leaked connections. These listeners log a short stack so
        # we can find the call sites that caused a checkout without a later
        # checkin.
        try:
            sync_engine = _engine.sync_engine

            logger = logging.getLogger(__name__)

            def _on_checkout(dbapi_con: Any, con_record: Any, con_proxy: Any) -> None:
                """Pool checkout listener with improved caller attribution.

                Captures a longer stack, then picks the first frame that comes
                from the project (a path containing '/bot/') to make it easier
                to find the app-level call site that triggered the checkout.
                """
                task = asyncio.current_task()
                task_name = getattr(task, "get_name", lambda: None)() if task is not None else None
                logger.debug(
                    "POOL checkout: con=%s record=%s task=%s task_name=%s",
                    getattr(dbapi_con, "pid", id(dbapi_con)),
                    con_record,
                    repr(task),
                    task_name,
                )

            def _on_checkin(dbapi_con: Any, con_record: Any) -> None:
                task = asyncio.current_task()
                task_name = getattr(task, "get_name", lambda: None)() if task is not None else None
                logger.debug(
                    "POOL checkin: con=%s record=%s task=%s task_name=%s",
                    getattr(dbapi_con, "pid", id(dbapi_con)),
                    con_record,
                    repr(task),
                    task_name,
                )

            # Listen on the underlying (sync) engine's pool events.
            event.listen(sync_engine, "checkout", _on_checkout)
            event.listen(sync_engine, "checkin", _on_checkin)
        except Exception:
            logging.getLogger(__name__).exception("Failed to attach pool event listeners")
        _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    get_engine()
    assert _session_factory is not None
    return _session_factory


@asynccontextmanager
async def get_session() -> AsyncIterator[AsyncSession]:
    """Provide a new AsyncSession."""
    logger = logging.getLogger(__name__)
    factory = get_session_factory()
    session = factory()
    # capture a short creation stack to help track callers that don't close sessions
    logger.debug("get_session: created session id=%s", id(session))
    try:
        yield session
    finally:
        try:
            await session.close()
        finally:
            logger.debug("get_session: closed session id=%s", id(session))


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

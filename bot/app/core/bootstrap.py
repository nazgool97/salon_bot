"""
Runtime bootstrap helpers used in tests.

Provides idempotent initial population helpers for services and masters.
Tests patch `get_session` via monkeypatch; we therefore import it lazily.
"""

from typing import Iterable
import os
from sqlalchemy.ext.asyncio import AsyncSession

from bot.app.domain import models

__all__ = ["init_services", "init_masters", "maybe_start_scheduler"]


async def _upsert_services(
    existing_ids: set[str], session: AsyncSession, specs: Iterable[tuple[str, str, int, str]]
) -> None:
    """Добавляет недостающие сервисы."""
    for sid, name, price, category in specs:
        if sid in existing_ids:
            continue
        session.add(
            models.Service(
                id=sid,
                name=name,
                category=category,
                price_cents=price,
                currency="UAH",
            )
        )


async def init_services() -> None:
    """Insert baseline services if missing (idempotent)."""
    from sqlalchemy import select
    from bot.app.core.db import get_session  # lazy import для тестов

    # Do not seed default services in production by default. This behaviour
    # is guarded by the RUN_BOOTSTRAP environment variable so CI/tests can
    # still opt-in. If RUN_BOOTSTRAP is not truthy, return early.
    if os.getenv("RUN_BOOTSTRAP", "0").lower() not in {"1", "true", "yes"}:
        return

    async with get_session() as session:
        result = await session.execute(select(models.Service.id))
        existing = {row[0] for row in result.all()}
        await _upsert_services(existing, session, DEFAULT_SERVICES)
        await session.commit()


async def _ensure_master(session: AsyncSession, telegram_id: int, name: str) -> models.Master:
    """Создаёт мастера, если его ещё нет."""
    from sqlalchemy import select

    result = await session.execute(
        select(models.Master).where(models.Master.telegram_id == telegram_id)
    )
    obj = result.scalars().first()
    if obj:
        return obj
    obj = models.Master(telegram_id=telegram_id, name=name)
    session.add(obj)
    return obj



from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
import bot.config as cfg

# Указываем parse_mode через DefaultBotProperties
bot = Bot(
    token=getattr(cfg, "BOT_TOKEN", ""),
    default=DefaultBotProperties(parse_mode="HTML")
)

dp = Dispatcher()





async def init_masters() -> None:
    """Insert baseline masters and link them to all services (idempotent)."""
    from sqlalchemy import select
    from bot.app.core.db import get_session  # lazy import для тестов

    async with get_session() as session:
        # Load existing services (do not auto-create DEFAULT_SERVICES here).
        svc_result = await session.execute(select(models.Service.id))
        service_ids = [row[0] for row in svc_result.all()]

        # Load existing masters only. Do NOT create MasterService links
        # automatically; linking must be performed explicitly via the admin
        # UI (or CLI). This avoids accidental population of production data.
        try:
            res = await session.execute(select(models.Master))
            masters = res.scalars().all() or []
        except Exception:
            masters = []


async def maybe_start_scheduler() -> None:  # pragma: no cover
    """Placeholder to satisfy legacy imports; real scheduler lives elsewhere."""
    return None
"""Background worker to send 24h visit reminders.

Scans upcoming bookings and sends a reminder message about 24 hours before start.
Marks a per-booking flag to avoid duplicate notifications.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Awaitable, Callable, Optional

from aiogram import Bot
from sqlalchemy import select, update

from bot.app.core.db import get_session
from bot.app.domain.models import Booking, BookingStatus, Service, User, Master, BookingItem
from bot.app.services.shared_services import safe_get_locale
from bot.app.translations import t
import bot.config as cfg

logger = logging.getLogger(__name__)


async def _remind_once(now_utc: datetime, bot: Bot) -> int:
    try:
        # Window: all future bookings within the next 24h that are not reminded yet
        window_utc = now_utc + timedelta(hours=24)
        async with get_session() as session:
            stmt = (
                select(Booking, User.telegram_id)
                .join(User, User.id == Booking.user_id)
                .where(
                    Booking.status.in_([BookingStatus.CONFIRMED, BookingStatus.PAID]),
                    Booking.remind_24h_sent.is_(False),
                    Booking.starts_at > now_utc,
                    Booking.starts_at <= window_utc,
                )
            )
            rows = list((await session.execute(stmt)).all())
            count = 0
            for booking, chat_id in rows:
                try:
                    # Resolve locale and details
                    # Resolve locale via centralized safe_get_locale (handles fallbacks)
                    lang = await safe_get_locale(int(chat_id))

                    # Load service/master names (support multi-service via BookingItem list)
                    svc = await session.get(Service, booking.service_id)
                    m = await session.get(Master, booking.master_id)
                    # Try to collect names from BookingItem; fallback to single service
                    try:
                        rows = list((await session.execute(
                            select(BookingItem.service_id, Service.name)
                            .join(Service, Service.id == BookingItem.service_id)
                            .where(BookingItem.booking_id == booking.id)
                        )).all())
                        if rows:
                            service_name = " + ".join([r[1] or r[0] for r in rows])
                        else:
                            service_name = getattr(svc, "name", None) or t("service_label", lang)
                    except Exception:
                        service_name = getattr(svc, "name", None) or t("service_label", lang)
                    master_name = getattr(m, "name", None) or t("master_label", lang)

                    # Localized time
                    LOCAL_TZ = getattr(cfg, "LOCAL_TZ", None)
                    try:
                        dt_local = booking.starts_at.astimezone(LOCAL_TZ) if LOCAL_TZ else booking.starts_at
                        time_txt = f"{dt_local:%H:%M}"
                    except Exception:
                        time_txt = "--:--"

                    title = t("reminder_24h_title", lang)
                    if title == "reminder_24h_title":
                        title = {
                            "uk": "Нагадування про запис",
                            "ru": "Напоминание о записи",
                            "en": "Appointment reminder",
                        }.get(lang, "Appointment reminder")
                    body = t("reminder_24h_body", lang).format(time=time_txt, service=service_name, master=master_name)
                    text = f"<b>{title}</b>\n\n{body}"

                    try:
                        from bot.app.services.shared_services import _safe_send
                        ok = await _safe_send(bot, chat_id, text)
                        if not ok:
                            logger.warning("Failed to send 24h reminder to %s for booking %s", chat_id, booking.id)
                            # Skip marking sent on delivery failure
                            continue
                    except Exception as se:
                        logger.warning("Failed to send 24h reminder to %s for booking %s: %s", chat_id, booking.id, se)
                        continue

                    await session.execute(
                        update(Booking)
                        .where(Booking.id == booking.id)
                        .values(remind_24h_sent=True)
                    )
                    count += 1
                except Exception as ie:
                    logger.exception("Error processing reminder for booking %s: %s", getattr(booking, "id", "?"), ie)
            if count:
                await session.commit()
            return count
    except Exception as e:
        logger.error("Reminder sweep failed: %s", e)
        return 0


async def _run_loop(stop_event: asyncio.Event, bot: Bot, interval_seconds: int) -> None:
    # small initial delay
    try:
        await asyncio.sleep(2)
    except Exception:
        pass
    while not stop_event.is_set():
        try:
            await _remind_once(datetime.now(UTC), bot)
        except Exception as e:
            logger.exception("Reminders worker iteration error: %s", e)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            continue
        except Exception:
            break


async def start_reminders_worker(bot: Bot) -> Callable[[], Awaitable[None]]:
    """Start the reminders worker and return an async stop() function."""
    settings = getattr(cfg, "SETTINGS", {})
    interval_seconds = int(settings.get("reminders_check_seconds", 60))
    stop_event: asyncio.Event = asyncio.Event()
    task = asyncio.create_task(_run_loop(stop_event, bot, interval_seconds), name="reminders-worker")

    async def _stop() -> None:
        try:
            stop_event.set()
            try:
                await asyncio.wait_for(task, timeout=5)
            except Exception:
                task.cancel()
        except Exception:
            pass

    logger.info("Reminders worker started (interval=%ss)", interval_seconds)
    return _stop


async def stop_reminders_worker(stop_callable: Optional[Callable[[], Awaitable[None]]] = None) -> None:
    if stop_callable:
        await stop_callable()


__all__ = ["start_reminders_worker", "stop_reminders_worker"]
